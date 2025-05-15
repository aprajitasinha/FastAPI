from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_middleware_logs_request(capfd):
    response = client.get("/")
    assert response.status_code == 200
    out, err = capfd.readouterr()
    assert "Incoming request: http://127.0.0.1:5003/" in out
    assert "Response status: 200" in out
