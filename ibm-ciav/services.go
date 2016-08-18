/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the IBM India Pvt Ltd, Version 1.0 (the "License");
*/

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/ibm-ciav/address"
	"github.com/ibm-ciav/customer"
	"github.com/ibm-ciav/identification"
	"github.com/ibm-ciav/kyc"
)

type ServicesChaincode struct {
}

/*
   Deploy KYC data model
*/
func (t *ServicesChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
	identification.CreateTable(stub, args)
	customer.CreateTable(stub, args)
	kyc.CreateTable(stub, args)
	address.CreateTable(stub, args)
	return nil, nil
}

/*
  Add Customer record
*/
func (t *ServicesChaincode) addCIAV(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	if len(args) != 43 {
		return nil, errors.New("Incorrect number of arguments. Expecting 43")
	}
	// Common
	customer_id := args[0]
	source := args[1]

	// Identification
	identity_number := args[2]
	poi_type := args[3]
	poi_doc := args[4]
	poi_expiry_date := args[5]

	identity_number2 := args[28]
	poi_type2 := args[29]
	poi_doc2 := args[30]
	poi_expiry_date2 := args[31]

	// Customer personal details
	first_name := args[6]
	last_name := args[7]
	sex := args[8]
	email_id := args[9]
	dob := args[10]
	phone_number := args[11]
	occupation := args[12]
	annual_income := args[13]
	income_source := args[14]

	//kyc
	kyc_status := args[15]
	last_updated := args[16]

	// Address
	address_id := args[17]
	address_type := args[18]
	door_number := args[19]
	street := args[20]
  locality := args[21]
	city :=args[22]
	state := args[23]
	pincode := args[24]
	poa_type := args[25]
	poa_doc := args[26]
	poa_expiry_date := args[27]

	address_id2 := args[32]
	address_type2 := args[33]
	door_number2 := args[34]
	street2 := args[35]
	locality2 := args[36]
	city2 :=args[37]
	state2 := args[38]
	pincode2 := args[39]
	poa_type2 := args[40]
	poa_doc2 := args[41]
	poa_expiry_date2 := args[42]

	identification.AddIdentification(stub, []string{customer_id, identity_number, poi_type, poi_doc, poi_expiry_date, source})
	customer.AddCustomer(stub, []string{customer_id, first_name, last_name, sex, email_id, dob, phone_number, occupation, annual_income, income_source, source})
	kyc.AddKYC(stub, []string{customer_id, kyc_status, last_updated, source})
	address.AddAddress(stub, []string{customer_id, address_id, address_type, door_number, street, locality, city, state, pincode, poa_type, poa_doc, poa_expiry_date, source})

	if args[28] != "" {
		identification.AddIdentification(stub, []string{customer_id, identity_number2, poi_type2, poi_doc2, poi_expiry_date2, source})
	}
	if args[32] != "" {
		address.AddAddress(stub, []string{customer_id, address_id2, address_type2, door_number2, street2, locality2, city2, state2, pincode2, poa_type2, poa_doc2, poa_expiry_date2, source})
	}
	return nil, nil
}

/*
 Update customer record
*/
func (t *ServicesChaincode) updateCIAV(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	if len(args) != 43 {
		return nil, errors.New("Incorrect number of arguments. Expecting 43")
	}
	// Common
	customer_id := args[0]
	source := args[1]

	// Identification
	identity_number := args[2]
	poi_type := args[3]
	poi_doc := args[4]
	poi_expiry_date := args[5]

	identity_number2 := args[28]
	poi_type2 := args[29]
	poi_doc2 := args[30]
	poi_expiry_date2 := args[31]

	// Customer personal details
	first_name := args[6]
	last_name := args[7]
	sex := args[8]
	email_id := args[9]
	dob := args[10]
	phone_number := args[11]
	occupation := args[12]
	annual_income := args[13]
	income_source := args[14]

	//kyc
	kyc_status := args[15]
	last_updated := args[16]

	// Address
	address_id := args[17]
	address_type := args[18]
	door_number := args[19]
	street := args[20]
	locality := args[21]
	city := args[22]
	state := args[23]
	pincode := args[24]
	poa_type := args[25]
	poa_doc := args[26]
	poa_expiry_date := args[27]

	address_id2 := args[32]
	address_type2 := args[33]
	door_number2 := args[34]
	street2 := args[35]
	locality2 := args[36]
	city2 :=args[37]
	state2 := args[38]
	pincode2 := args[39]
	poa_type2 := args[40]
	poa_doc2 := args[41]
	poa_expiry_date2 := args[42]

	identification.UpdateIdentification(stub, []string{customer_id, identity_number, poi_type, poi_doc, poi_expiry_date, source})
	customer.UpdateCustomer(stub, []string{customer_id, first_name, last_name, sex, email_id, dob, phone_number, occupation, annual_income, income_source, source})
	kyc.UpdateKYC(stub, []string{customer_id, kyc_status, last_updated, source})
	address.UpdateAddress(stub, []string{customer_id, address_id, address_type, door_number, street, locality, city, state, pincode, poa_type, poa_doc, poa_expiry_date, source})

	if args[28] != "" {
		identification.UpdateIdentification(stub, []string{customer_id, identity_number2, poi_type2, poi_doc2, poi_expiry_date2, source})
	}
	if args[32] != "" {
		address.UpdateAddress(stub, []string{customer_id, address_id2, address_type2, door_number2, street2, locality2, city2, state2, pincode2, poa_type2, poa_doc2, poa_expiry_date2, source})
	}
	return nil, nil
}

/*
   Invoke : addCIAV and updateCIAV
*/
func (t *ServicesChaincode) Invoke(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {

	if function == "addCIAV" {
		// add customer
		return t.addCIAV(stub, args)
	} else if function == "updateCIAV" {
		// update customer
		return t.updateCIAV(stub, args)
	}

	return nil, errors.New("Received unknown function invocation")
}

/*
 		Get Customer record by customer id or PAN number
*/
func (t *ServicesChaincode) Query(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
	if function == "getCIAV" {
		return t.getCIAV(stub, args)
	}
	return nil, errors.New("Received unknown function invocation")
}

func (t *ServicesChaincode) getCIAV(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	var jsonResp string
	var customerIds []string
	var err error

	var identificationStr string
	var customerStr string
	var kycStr string
	var addressStr string
	if args[0] == "PAN" {
		customerIds, err = identification.GetCustomerID(stub, args[1])
		jsonResp = "["
		for i := range customerIds {
			customerId := customerIds[i]
			identificationStr, err = identification.GetIdentification(stub, customerId)
			customerStr, err = customer.GetCustomer(stub, customerId)
			kycStr, err = kyc.GetKYC(stub, customerId)
			addressStr, err = address.GetAddress(stub, customerId)

			if i != 0 {
				jsonResp = jsonResp + ","
			}
			jsonResp = jsonResp + "{\"Identification\":" + identificationStr +
				",\"PersonalDetails\":" + customerStr +
				",\"KYC\":" + kycStr +
				",\"address\":" + addressStr + "}"
		}
			jsonResp = jsonResp + "]"
	} else if args[0] == "CUST_ID" {
		customerId := args[1]
		identificationStr, err = identification.GetIdentification(stub, customerId)
		customerStr, err = customer.GetCustomer(stub, customerId)
		kycStr, err = kyc.GetKYC(stub, customerId)
		addressStr, err = address.GetAddress(stub, customerId)

		jsonResp = "{\"Identification\":" + identificationStr +
			",\"PersonalDetails\":" + customerStr +
			",\"KYC\":" + kycStr +
			",\"address\":" + addressStr + "}"
	} else {
		return nil, errors.New("Invalid arguments. Please query by CUST_ID or PAN")
	}

	bytes, err := json.Marshal(jsonResp)
	if err != nil {
		return nil, errors.New("Error converting kyc record")
	}
	return bytes, nil
}

func main() {
	err := shim.Start(new(ServicesChaincode))
	if err != nil {
		fmt.Printf("Error starting ServicesChaincode: %s", err)
	}
}
