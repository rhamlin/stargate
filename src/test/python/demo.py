


import json
import requests

url = "http://localhost:8080"
url = "https://sandbox.non-prod.appstax.com"

def printResult(result):
    print(json.dumps(json.loads(result.text), indent=True))


def push():
    resp = requests.post(url + "/test", data=str(open("/data/projects/stargate/src/main/resources/schema.conf").read()))
    if resp.status_code != 200:
        push()


def create():
    result = requests.post(url + "/test/Customer/create", json={
        "firstName": "Steve",
        # relation ops are: [link, unlink, replace], if none of these are specified, defaults to replace
        "addresses": {
            # child entities (to be linked, unlinked, etc) can be either creates (new) or updates (existing)
            # if neither create nor update are specified, defaults to crete
            "street": "kent st",
            "zip":"22046"
        },
        "orders":[
            # links parent customer to a new order, which is linked to any existing products with the name "widget"
            {
                "time": 12345,
                "products": {
                    "-link": {
                        "-update": {
                            "-match": ["name", "=", "widget"]
                        }
                    }
                }
            },
            # links parent customer to a new order with no products
            # this is the long-form version of "products": [], (since default behavior is /replace/create)
            {
                "total": 0,
                "products": {
                    "-replace": {
                        "-create": []
                    }
                }
            }
        ]
    })
    printResult(result)

def get():
    # get all Customers with firstName=Daniel, include any related addresses and orders in results
    result = requests.post(url + "/test/Customer/get", json={
        "-match":["firstName","=", "Steve"],
        "addresses":{},
        "orders":{}})
    printResult(result)

def update():
    result = requests.post(url + "/test/Customer/update", json={
        "-match":["firstName","=","Steve"],
        "lastName": "Danger",
        "addresses":{
            "-update":{
                "-match":["customers.firstName","=","Steve"],
                "street":"other st"}}})
    printResult(result)

def predefinedGet(name = "Steve"):
    result = requests.post(url + "/test/customerByFirstName", json={
        "-match":{
            "customerName": name
        }
    })
    printResult(result)



def demo1():
    push()
    print("\n\ntest 1")
    create()
    get()
    print("\n\ntest 2")
    update()
    get()
    print("\n\ntest 3")
    create()
    get()
    print("\n\ntest 4")
    predefinedGet()


