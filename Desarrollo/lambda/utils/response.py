def response_error(message):
    """"status": "ERROR","""
    return {
        "status": "ERROR",
        "message": message
    }

def response_ok(data):
    return {
        "status": "OK",
        "data": data
    }