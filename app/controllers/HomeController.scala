package controllers

import play.api.mvc._
import play.filters.csrf._
import javax.inject._

@Singleton
class HomeController @Inject()(cc: ControllerComponents)
  extends AbstractController(cc) {

  def index: Action[AnyContent] = Action { implicit request =>
    val token = CSRF.getToken.get
    Ok(views.html.index()(request, token))
  }
}