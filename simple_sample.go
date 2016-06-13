package main

import (
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	//"encoding/json"
)


type  SimpleChaincode struct {
}


type Patient struct {
	
	Id               string  `json:"id"`
	Name            string `json:"name"`
	Address           string `json:"address"`
	CreatedBy        string `json:"createdby"`
}



func (t *SimpleChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    if len(args) != 1 {
        return nil, errors.New("Incorrect number of arguments. Expecting 1")
    }

    err := stub.PutState("hello_world", []byte(args[0]))
    if err != nil {
        return nil, err
    }

    return nil, nil
}

func (t *SimpleChaincode) Invoke(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    fmt.Println("invoke is running " + function)

    // Handle different functions
    if function == "init" {
        return t.Init(stub, "init", args)
    } else if function == "write" {
        return t.write(stub, args)
    } else if function == "create_patient" {
    	return t.create_patient(stub,args)
    }
    fmt.Println("invoke did not find func: " + function)

    return nil, errors.New("Received unknown function invocation")
}

func (t *SimpleChaincode) write(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
    var name, value string
    var err error
    fmt.Println("running write()")

    if len(args) != 2 {
        return nil, errors.New("Incorrect number of arguments. Expecting 2. name of the variable and value to set")
    }

    name = args[0]                            //rename for fun
    value = args[1]
    err = stub.PutState(name, []byte(value))  //write the variable into the chaincode state
    if err != nil {
        return nil, err
    }
    return nil, nil
}


func (t *SimpleChaincode) Query(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    fmt.Println("query is running " + function)

    // Handle different functions
    if function == "read" {                            //read a variable
        return t.read(stub, args)
    }
    fmt.Println("query did not find func: " + function)

    return nil, errors.New("Received unknown function query")
}


func (t *SimpleChaincode) read(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
    var name, jsonResp string
    var err error

    if len(args) != 1 {
        return nil, errors.New("Incorrect number of arguments. Expecting name of the var to query")
    }

    name = args[0]
    valAsbytes, err := stub.GetState(name)
    if err != nil {
        jsonResp = "{\"Error\":\"Failed to get state for " + name + "\"}"
        return nil, errors.New(jsonResp)
    }

    return valAsbytes, nil
}

func (t *SimpleChaincode) create_patient(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
   
   var err error
   id :=  "\"id\":\""+args[0]+"\", "
   name := "\"name\":\""+args[1]+"\", "
   address :=  "\"address\":\""+args[2]+"\""
   
   json_string :="{"+id+name+address+"}"
   
   
	  err= stub.PutState(args[0],[]byte(json_string))
	  
   if err != nil {
        return nil, err
    }
   
    return nil, nil
}

func (t *SimpleChaincode) retrieve_patient(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
  // var p Patient
   
    return nil, nil
}


func main() {
    err := shim.Start(new(SimpleChaincode))
    if err != nil {
        fmt.Printf("Error starting Simple chaincode: %s", err)
    }
}
