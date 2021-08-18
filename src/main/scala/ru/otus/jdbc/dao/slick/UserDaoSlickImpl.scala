package ru.otus.jdbc.dao.slick


import ru.otus.jdbc.model.{Role, User}
import slick.dbio.Effect
import slick.jdbc.PostgresProfile.api._
import slick.sql.FixedSqlAction

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success


class UserDaoSlickImpl(db: Database)(implicit ec: ExecutionContext) {

  import UserDaoSlickImpl._

  def getUser(userId: UUID): Future[Option[User]] = {
    val res: DBIOAction[Option[User], NoStream, Effect.Read] = for {
      user <- users.filter(user => user.id === userId).result.headOption
      roles <- usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)
    } yield user.map(_.toUser(roles))

    db.run(res)
  }

  def createUser(user: User): Future[User] = {

    val action: DBIOAction[User, NoStream, Effect.Write] =
      for {
        userId <- (users returning users.map(_.id)) += UserRow.fromUser(user)
        _ <- usersToRoles ++= user.roles.map(userId -> _)
      } yield user.copy(id = Some(userId))

    db.run(action)

  }


  def updateUser(user: User): Future[Unit] = {
    user.id match {
      case Some(userId) =>
        val updateUser: FixedSqlAction[Int, NoStream, Effect.Write] = users
          .filter(_.id === userId)
          .map(u => (u.firstName, u.lastName, u.age))
          .update((user.firstName, user.lastName, user.age))

        val deleteRoles: FixedSqlAction[Int, NoStream, Effect.Write] = usersToRoles.filter(_.usersId === userId).delete
        val insertRoles: FixedSqlAction[Option[Int], NoStream, Effect.Write] = usersToRoles ++= user.roles.map(userId -> _)

        val action: DBIOAction[Unit, NoStream, Effect.Write with Effect] =
          updateUser >> deleteRoles >> insertRoles >> DBIO.successful(())

        db.run(action)
      case None => Future.successful(())
    }
  }

  def deleteUser(userId: UUID): Future[Option[User]] = {
    val deleteRoles = usersToRoles.filter(_.usersId === userId).delete
    val deleteUser = users.filter(_.id === userId).delete
    val findUser = users.filter(_.id === userId).take(1).result.headOption
    val findRoles = usersToRoles.filter(_.usersId === userId).map(_.rolesCode).result.map(_.toSet)

    val action = for {
      roles <- findRoles
      user <- findUser
      _ <- deleteRoles
      _ <- deleteUser
    } yield user.map(_.toUser(roles))


    db.run(action)

  }

  private def findByCondition(condition: Users => Rep[Boolean]): Future[Vector[User]] = {
    val action =
      users.filter(condition).joinLeft(usersToRoles).on(_.id === _.usersId).map {
        case (users: Users, usersToRoles: Rep[Option[UsersToRoles]]) =>
          users -> usersToRoles.map(_.rolesCode)
      }
        .result
        .map {
          seq: Seq[(UserRow, Option[Role])] =>
            seq.groupBy {
              case (userRow: UserRow, _) => userRow
            }.map {
              case (userRow: UserRow, grouped) =>
                val roles = grouped.flatMap {
                  case (_, role: Option[Role]) => role
                }.toSet
                userRow.toUser(roles)
            }.toVector
        }

    db.run(action)

  }

  def findByLastName(lastName: String): Future[Seq[User]] =
    findByCondition(_.lastName === lastName)

  def findAll(): Future[Seq[User]] = findByCondition(_ => true: Rep[Boolean])

  private[jdbc] def deleteAll(): Future[Unit] = {

    val action = usersToRoles.delete >> users.delete >> DBIO.successful(())

    db.run(action)
  }

}

object UserDaoSlickImpl {
  implicit val rolesType: BaseColumnType[Role] = MappedColumnType.base[Role, String](
    {
      case Role.Reader => "reader"
      case Role.Manager => "manager"
      case Role.Admin => "admin"
    },
    {
      case "reader" => Role.Reader
      case "manager" => Role.Manager
      case "admin" => Role.Admin
    }
  )


  case class UserRow(
                      id: Option[UUID],
                      firstName: String,
                      lastName: String,
                      age: Int
                    ) {
    def toUser(roles: Set[Role]): User = User(id, firstName, lastName, age, roles)
  }

  object UserRow extends ((Option[UUID], String, String, Int) => UserRow) {
    def fromUser(user: User): UserRow = UserRow(user.id, user.firstName, user.lastName, user.age)
  }

  class Users(tag: Tag) extends Table[UserRow](tag, "users") {
    val id = column[UUID]("id", O.PrimaryKey, O.AutoInc)
    val firstName = column[String]("first_name")
    val lastName = column[String]("last_name")
    val age = column[Int]("age")

    val * = (id.?, firstName, lastName, age).mapTo[UserRow]
  }

  val users: TableQuery[Users] = TableQuery[Users]

  class UsersToRoles(tag: Tag) extends Table[(UUID, Role)](tag, "users_to_roles") {
    val usersId = column[UUID]("users_id")
    val rolesCode = column[Role]("roles_code")

    val * = (usersId, rolesCode)
  }

  val usersToRoles = TableQuery[UsersToRoles]
}
