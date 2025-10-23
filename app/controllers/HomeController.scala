package controllers

import play.api.mvc.*
import play.filters.csrf.*

import javax.inject.*

@Singleton
class HomeController @Inject() (cc: ControllerComponents) extends AbstractController(cc) {

  def index: Action[AnyContent] = Action { implicit request =>
    val token = CSRF.getToken.get
    Ok(views.html.index()(request, token))
  }
}
