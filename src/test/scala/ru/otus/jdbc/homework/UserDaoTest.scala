package ru.otus.jdbc.homework

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import org.flywaydb.core.Flyway
import org.postgresql.jdbc.PgConnection
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Seconds, Span}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.slf4j.{Logger, LoggerFactory}
import ru.otus.jdbc.dao.slick.UserDaoSlickImpl
import ru.otus.jdbc.model.{Role, User}
import slick.jdbc.JdbcBackend.Database

import java.util.{Properties, UUID}
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class UserDaoSlickImplTest
  extends AnyFreeSpec
    with ScalaCheckDrivenPropertyChecks
    with ScalaFutures
    with BeforeAndAfterAll
    with WithResource {

  import scala.concurrent.duration._

  val timeout: FiniteDuration = 10.seconds

  val log: Logger = LoggerFactory.getLogger("test.UserDaoSlickImplTest")

  val database: EmbeddedPostgres = EmbeddedPostgres.builder().start()

  database.getPostgresDatabase.getConnection.asInstanceOf[PgConnection].setPrepareThreshold(100)

  val dbDriver = "org.postgresql.Driver"
  //val dbDriver   = "com.p6spy.engine.spy.P6SpyDriver"
  val dbUser = "postgres"
  val dbPassword = ""
  val dbUrl: String = database.getJdbcUrl(dbUser, "postgres")

  lazy val flyway: Flyway =
    Flyway
      .configure()
      .dataSource(dbUrl, dbUser, dbPassword)
      .load()

  class Fixture extends AutoCloseable {

    val db = Database.forURL(dbUrl, dbUser, dbPassword)

    def setup(): Unit = {
      initDB()
    }

    override def close(): Unit = {
      clearDb()
    }

  }

  def withFixture(test: Fixture => Any): Any =
    withResource(new Fixture) { fixture =>
      fixture.setup()
      test(fixture)
    }

  implicit val genRole: Gen[Role] = Gen.oneOf(Role.Admin, Role.Manager, Role.Reader)
  implicit val arbitraryRole: Arbitrary[Role] = Arbitrary(genRole)

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = scaled(Span(10, Seconds)))

  implicit lazy val arbString: Arbitrary[String] = Arbitrary(arbitrary[List[Char]] map (_.filter(_ != 0).mkString))

  val rnd = scala.util.Random
  implicit val genUser: Gen[User] = for {
    id <- Gen.option(Gen.uuid)
    firstName <- Gen.alphaStr.map(_.take(5)) // s"firstName-${rnd.nextInt(10)}" // arbitrary[String]
    lastName <- Gen.alphaStr.map(_.take(5)) //s"firstName-${rnd.nextInt(10)}" //arbitrary[String]
    age <- arbitrary[Int]
    roles <- arbitrary[Seq[Role]]
  } yield User(id = id, firstName = firstName, lastName = lastName, age = age, roles = roles.toSet)

  implicit val arbitraryUser: Arbitrary[User] = Arbitrary(genUser)


  "getUser" - {
    "create and get unknown user" in withFixture { fixture =>
      forAll { (users: Seq[User], userId: UUID) =>
        val dao = new UserDaoSlickImpl(fixture.db)
        users.foreach(dao.createUser(_).futureValue)

        dao.getUser(userId).futureValue shouldBe None
        Await.ready(dao.deleteAll(), timeout)
      }
    }
  }


  "updateUser" - {
    "update known user - keep other users the same" in withFixture { fixture =>
      forAll { (users: Seq[User], user1: User, user2: User) =>
        import Role._
        import java.util.UUID

        val dao = new UserDaoSlickImpl(fixture.db)

        val otherUsers: Seq[User] = users.map(dao.createUser(_).futureValue)

        val newUser: User = dao.createUser(user1).futureValue
        val toUpdate = user2.copy(id = newUser.id)


        Await.ready(dao.updateUser(toUpdate),timeout)

        dao.getUser(toUpdate.id.get).futureValue shouldBe Some(toUpdate)

        otherUsers.foreach { u =>
          dao.getUser(u.id.get).futureValue shouldBe Some(u)
        }

        Await.ready(dao.deleteAll(), timeout)
      }
    }
  }

  "delete known user - keep other users the same" in withFixture { fixture =>
    forAll { (users1: Seq[User], user1: User) =>
      val dao = new UserDaoSlickImpl(fixture.db)
      val createdUsers1 = users1.map(dao.createUser(_).futureValue)
      val createdUser = dao.createUser(user1).futureValue

      dao.getUser(createdUser.id.get).futureValue shouldBe Some(createdUser)
      dao.deleteUser(createdUser.id.get).futureValue shouldBe Some(createdUser)
      dao.getUser(createdUser.id.get).futureValue shouldBe None

      createdUsers1.foreach { u => dao.getUser(u.id.get).futureValue shouldBe Some(u) }
      Await.ready(dao.deleteAll(), timeout)
    }
  }

  "findByLastName" in withFixture { fixture =>
    forAll { (users1: Seq[User], lastName: String, users2: Seq[User]) =>
      val dao = new UserDaoSlickImpl(fixture.db)
      val withOtherLastName = users1.filterNot(_.lastName == lastName)
      val withLastName = users2.map(_.copy(lastName = lastName))

      withOtherLastName.foreach(dao.createUser(_).futureValue)
      val createdWithLasName = withLastName.map(dao.createUser(_).futureValue)

      dao.findByLastName(lastName).futureValue.toSet shouldBe createdWithLasName.toSet
      Await.ready(dao.deleteAll(), timeout)
    }
  }

  "findAll" in withFixture { fixture =>
    forAll { users: Seq[User] =>
      val dao = new UserDaoSlickImpl(fixture.db)

      val createdUsers: Seq[User] = users.map(dao.createUser(_).futureValue)

      dao.findAll().futureValue.toSet shouldBe createdUsers.toSet

      Await.ready(dao.deleteAll(), timeout)
    }

  }

  def initDB(): Unit = {
    migrate()
  }

  def clearDb(): Unit = {
    flyway.clean()
  }

  @tailrec
  final def migrate(): Unit = {
    try {
      flyway.migrate()

      log.info("Database migration & connection test complete")

    } catch {
      case e: Exception =>
        log.warn("Database not available, waiting 5 seconds to retry...", e)
        Thread.sleep(5000)
        migrate()
    }
  }


}
