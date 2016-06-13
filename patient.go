package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"encoding/json"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/url"
    "io/ioutil"
	"regexp"
	
)

//==============================================================================================================================
//	 Participant types - Each participant type is mapped to an integer which we use to compare to the value stored in a
//						 user's eCert
//==============================================================================================================================
const   PCP      =  1
const   HOSPITAL   =  2
const   LAB =  3
const   INSURER  =  4


//==============================================================================================================================
//	 Status types - Asset lifecycle is broken down into 5 statuses, this is part of the business logic to determine what can 
//					be done to the vehicle at points in it's lifecycle
//==============================================================================================================================
const   STATE_PERFORM_DIAGONISIS  			=  0
const   STATE_TREATEMENT			=  1
const   STATE_TREATED 	=  2


//==============================================================================================================================
//	 Structure Definitions 
//==============================================================================================================================
//	Chaincode - A blank struct for use with Shim (A HyperLedger included go file used for get/put state
//				and other HyperLedger functions)
//==============================================================================================================================
type  SimpleChaincode struct {
}

//==============================================================================================================================
//	Patient - Defines the structure for a Patient. JSON on right tells it what JSON fields to map to
//			  that element when reading a JSON object into the struct e.g. JSON make -> Struct Make.
//==============================================================================================================================
type Patient struct {
	//Name            string `json:"name"`
	//Gender           string `json:"gender"`
	//DOB             string `json:"dob"`
	Id               string  `json:"id"`					
	//Contact           string `json:"contact"`
	CreatedBy        string `json:"createdby"`
}

//==============================================================================================================================
//	ECertResponse - Struct for storing the JSON response of retrieving an ECert. JSON OK -> Struct OK
//==============================================================================================================================
type ECertResponse struct {
	OK string `json:"OK"`
}					

//==============================================================================================================================
//	Init Function - Called when the user deploys the chaincode																	
//==============================================================================================================================
func (t *SimpleChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
	
	//Args
	//				0
	//			peer_address
	
	
	err := stub.PutState("Peer_Address", []byte(args[0]))
															if err != nil { return nil, errors.New("Error storing peer address") }
	
	return nil, nil
}

//==============================================================================================================================
//	 General Functions
//==============================================================================================================================
//	 get_ecert - Takes the name passed and calls out to the REST API for HyperLedger to retrieve the ecert
//				 for that user. Returns the ecert as retrived including html encoding.
//==============================================================================================================================
func (t *SimpleChaincode) get_ecert(stub *shim.ChaincodeStub, name string) ([]byte, error) {
	
	var cert ECertResponse
	
	peer_address, err := stub.GetState("Peer_Address")
															if err != nil { return nil, errors.New("Error retrieving peer address") }

	response, err := http.Get("http://"+string(peer_address)+"/registrar/"+name+"/ecert") 	// Calls out to the HyperLedger REST API to get the ecert of the user with that name
    
															if err != nil { return nil, errors.New("Error calling ecert API") }
	
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)					// Read the response from the http callout into the variable contents
	
															if err != nil { return nil, errors.New("Could not read body") }
	
	err = json.Unmarshal(contents, &cert)
	
															if err != nil { return nil, errors.New("Could not retrieve ecert for user: "+name) }
															
	return []byte(string(cert.OK)), nil
}

//==============================================================================================================================
//	 get_caller - Retrieves the username of the user who invoked the chaincode.
//				  Returns the username as a string.
//==============================================================================================================================


func (t *SimpleChaincode) get_username(stub *shim.ChaincodeStub) (string, error) {

	bytes, err := stub.GetCallerCertificate();
															if err != nil { return "", errors.New("Couldn't retrieve caller certificate") }
	x509Cert, err := x509.ParseCertificate(bytes);				// Extract Certificate from result of GetCallerCertificate						
															if err != nil { return "", errors.New("Couldn't parse certificate")	}
															
	return x509Cert.Subject.CommonName, nil
}

//==============================================================================================================================
//	 check_affiliation - Takes an ecert as a string, decodes it to remove html encoding then parses it and checks the
// 				  		certificates common name. The affiliation is stored as part of the common name.
//==============================================================================================================================

func (t *SimpleChaincode) check_affiliation(stub *shim.ChaincodeStub, cert string) (int, error) {																																																					
	
	decodedCert, err := url.QueryUnescape(cert);    				// make % etc normal //
	
															if err != nil { return -1, errors.New("Could not decode certificate") }
	
	pem, _ := pem.Decode([]byte(decodedCert))           				// Make Plain text   //

	x509Cert, err := x509.ParseCertificate(pem.Bytes);				// Extract Certificate from argument //
														
															if err != nil { return -1, errors.New("Couldn't parse certificate")	}

	cn := x509Cert.Subject.CommonName
	
	res := strings.Split(cn,"\\")
	
	affiliation, _ := strconv.Atoi(res[2])
	
	return affiliation, nil
}

//==============================================================================================================================
//	 get_caller_data - Calls the get_ecert and check_role functions and returns the ecert and role for the
//					 name passed.
//==============================================================================================================================

func (t *SimpleChaincode) get_caller_data(stub *shim.ChaincodeStub) (string, int, error){

	user, err := t.get_username(stub)
																		if err != nil { return "", -1, err }
																		
	ecert, err := t.get_ecert(stub, user);					
																		if err != nil { return "", -1, err }

	affiliation, err := t.check_affiliation(stub,string(ecert));			
																		if err != nil { return "", -1, err }

	return user, affiliation, nil
}

//==============================================================================================================================
//	 retrieve_v5c - Gets the state of the data at v5cID in the ledger then converts it from the stored 
//					JSON into the Vehicle struct for use in the contract. Returns the Vehcile struct.
//					Returns empty v if it errors.
//==============================================================================================================================
func (t *SimpleChaincode) retrieve_patient(stub *shim.ChaincodeStub, Id string) (Patient, error) {
	
	var p Patient

	bytes, err := stub.GetState(Id)	;					
				
															if err != nil {	fmt.Printf("RETRIEVE_Patient: Failed to invoke patient_code: %s", err); return p, errors.New("RETRIEVE_Patient: Error retrieving Patient with id = " + Id) }

	fmt.Println("Patient dump form the BC SOMA "+string(bytes));
	
	err = json.Unmarshal(bytes, &p)	;	
	fmt.Println(err)
	fmt.Println("The error from the Json unmarshal")					

															if err != nil {	fmt.Printf("RETRIEVE_Patient: Corrupt patient record "+string(bytes)+": %s", err); return p, errors.New("RETRIEVE_Patient: Corrupt Patient record"+string(bytes))	}
	
	return p, nil
}

//==============================================================================================================================
// save_changes - Writes to the ledger the Patient struct passed in a JSON format. Uses the shim file's 
//				  method 'PutState'.
//==============================================================================================================================
func (t *SimpleChaincode) save_changes(stub *shim.ChaincodeStub, p Patient) (bool, error) {
	 
	bytes, err := json.Marshal(p)
	
																if err != nil { fmt.Printf("SAVE_CHANGES: Error converting Patient record: %s", err); return false, errors.New("Error converting Patient record") }

	err = stub.PutState(p.Id, bytes)
	
																if err != nil { fmt.Printf("SAVE_CHANGES: Error storing Patient record: %s", err); return false, errors.New("Error storing Patient record") }
	
	return true, nil
}

//==============================================================================================================================
//	 Router Functions
//==============================================================================================================================
//	Invoke - Called on chaincode invoke. Takes a function name passed and calls that function. Converts some
//		  initial arguments passed to other things for use in the called function e.g. name -> ecert
//==============================================================================================================================
func (t *SimpleChaincode) Invoke(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
	
	caller, caller_affiliation, err := t.get_caller_data(stub)

	if err != nil { return nil, errors.New("Error retrieving caller information")}

	//if function == "create_patient" { return t.create_patient(stub, caller, caller_affiliation, args[0],args[1],args[2],args[3],args[4])
	if function == "create_patient" { return t.create_patient(stub, caller, caller_affiliation, args[0])
	} else { 																				// If the function is not a create then there must be a car so we need to retrieve the car.
		
		
		p,err := t.retrieve_patient(stub, args[0])
		fmt.Printf("INVOKE: Patient  Id exist: %s", p.Id)
		
			if err != nil { fmt.Printf("INVOKE: Error retrieving Id: %s", err); return nil, errors.New("Error retrieving Patient") }
	
		return nil, errors.New("Function of that name doesn't exist.")
			
	}
}
//=================================================================================================================================	
//	Query - Called on chaincode query. Takes a function name passed and calls that function. Passes the
//  		initial arguments passed are passed on to the called function.
//=================================================================================================================================	
func (t *SimpleChaincode) Query(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
	
	if len(args) != 1 { fmt.Printf("Incorrect number of arguments passed"); return nil, errors.New("QUERY: Incorrect number of arguments passed") }

	p, err := t.retrieve_patient(stub, args[0])
																							if err != nil { fmt.Printf("QUERY: Error retrieving Id: %s", err); return nil, errors.New("QUERY: Error retrieving id "+err.Error()) }
															
	caller, caller_affiliation, err := t.get_caller_data(stub)
															
	if function == "get_all" { 
			return t.get_all(stub, p, caller, caller_affiliation)
	} 
																							return nil, errors.New("Received unknown function invocation")
}

//=================================================================================================================================
//	 Create Function
//=================================================================================================================================									
//	 Create Vehicle - Creates the initial JSON for the vehcile and then saves it to the ledger.									
//=================================================================================================================================
func (t *SimpleChaincode) create_patient(stub *shim.ChaincodeStub, caller string, caller_affiliation int, Id string) ([]byte, error) {								

	var p Patient																																										
	
	id	           := "\"id\":\""+Id+"\", "							// Variables to define the JSON
	//name           := "\"Name\":\""+Name+"\", "
	//gender          := "\"Gender\":\""+Gender+"\", "
	//dob            := "\"DOB\":\""+DOB+"\", "
	createdby          := "\"createdby\":\""+caller+"\", "
	//contact         := "\"Contact\":\""+Contact+"\", "
	
	//patient_json := "{"+id+name+gender+dob+createdby+contact+"}" 	// Concatenates the variables to create the total JSON object
	
	patient_json := "{"+id+createdby+"}"
	
	matched, err := regexp.Match("^[A-z][A-z][0-9]{7}", []byte(Id))  				// matched = true if the v5cID passed fits format of two letters followed by seven digits
	
																		if err != nil { fmt.Printf("CREATE_PATIENT: Invalid Id: %s", err); return nil, errors.New("Invalid Patient Id") }
	
	if 				Id  == "" 	 || 
					matched == false    {
																		fmt.Printf("CREATE_PATIENT: Invalid Patent ID provided");
																		return nil, errors.New("Invalid Id provided")
	}

	err = json.Unmarshal([]byte(patient_json), &p)							// Convert the JSON defined above into a patient object for go
	
																		if err != nil { return nil, errors.New("Invalid JSON object") }

	record, err := stub.GetState(p.Id) 								// If not an error then a record exists so cant create a new car with this V5cID as it must be unique
	
																		if record != nil { return nil, errors.New("Patient already exists") }
	
//	if 	caller_affiliation != AUTHORITY {							// Only the regulator can create a new v5c
//																		return nil, errors.New("Permission Denied")
//	}
	
	_, err  = t.save_changes(stub, p)									
			
																		if err != nil { fmt.Printf("CREATE_VEHICLE: Error saving changes: %s", err); return nil, errors.New("Error saving changes") }
																		
	return nil, nil

}




//=================================================================================================================================
//	 Read Functions
//=================================================================================================================================
//	 get_all
//=================================================================================================================================
func (t *SimpleChaincode) get_all(stub *shim.ChaincodeStub, p Patient, caller string, caller_affiliation int) ([]byte, error) {
	
	bytes, err := json.Marshal(p)
	
																if err != nil { return nil, errors.New("GET_ALL: Invalid Patient object") }
																
	if 		p.CreatedBy				== caller		||
			caller_affiliation	== HOSPITAL	{
			
					return bytes, nil		
	} else {
																return nil, errors.New("Permission Denied")	
	}

}

//=================================================================================================================================
//	 Main - main - Starts up the chaincode
//=================================================================================================================================
func main() {

	err := shim.Start(new(SimpleChaincode))
	
															if err != nil { fmt.Printf("Error starting Chaincode: %s", err) }
}
