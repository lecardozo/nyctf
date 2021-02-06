from inspect import getfullargspec
from .predictor import OnlinePredictor
try:
    from flask import Flask, jsonify, request
    from flasgger import Swagger
    HAS_SERVER_DEPENDENCIES = True
except ImportError:
    HAS_SERVER_DEPENDENCIES = False


def get_schema(predict_fn):
    return getfullargspec(predict_fn).annotations["features"]

def create_server(model_path: str):
    if not HAS_SERVER_DEPENDENCIES:
        raise ImportError("Server dependencies are missing. You must install with [server] extra requirements")

    predictor = OnlinePredictor.from_serialized(model_path)
    app = Flask(__name__)
    Swagger(app)
    input_schema = get_schema(predictor.predict)()

    @app.route("/predict", methods=["POST"])
    def predict():
        data = request.get_json()
        validation_errors = input_schema.validate(data)
        if validation_errors:
            return jsonify({
                "message": "Input data is wrong",
                "errors": validation_errors
            }), 400
        df = input_schema.load(data)
        prediction = predictor.predict(df)
        return jsonify(prediction), 200
    
    return app