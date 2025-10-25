package controllers

import play.api.mvc.*
import play.filters.csrf.*

import javax.inject.*

@Singleton
class HomeController @Inject() (cc: ControllerComponents) extends AbstractController(cc) {

  def index: Action[AnyContent] = Action { case given Request[AnyContent] =>
    CSRF.getToken.fold(NotFound("token not found")) { case given CSRF.Token =>
      Ok(views.html.index())
    }
  }
}
