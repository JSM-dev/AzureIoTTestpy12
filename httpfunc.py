from azure.functions import Blueprint, HttpRequest, HttpResponse

bp_httpfunc = Blueprint()

@bp_httpfunc.route(route="hello", methods=["GET", "POST"])
def hello_func(req: HttpRequest) -> HttpResponse:
    # logging.info(f"Requests version: {requests.__version__}")
    name = req.params.get("name")
    if not name and req.method == "POST":
        try:
            data = req.get_json()
            name = data.get("name")
        except Exception:
            name = None
    if not name:
        return HttpResponse("Please pass a name on the query string or in the request body", status_code=400)
    return HttpResponse(f"Hello, {name}!", status_code=200)