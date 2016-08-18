/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the IBM India Pvt Ltd, Version 1.0 (the "License");
*/

package kyc

import (
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
)

var myLogger = logging.MustGetLogger("customer_kyc_details")

/*
	Create KYC table
*/
func CreateTable(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Creating KYC Table...")
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	err := stub.CreateTable("KYC", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "customerId", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "kycStatus", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "lastUpdated", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "source", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating KYC table.")
	}
	myLogger.Debug("KYC table initialization done Successfully... !!! ")
	return nil, nil
}

/*
	Add KYC record
*/
func AddKYC(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Adding KYC record ...")

	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	customerId := args[0]
	kycStatus := args[1]
	lastUpdated := args[2]
	source := args[3]

	ok, err := stub.InsertRow("KYC", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			&shim.Column{Value: &shim.Column_String_{String_: kycStatus}},
			&shim.Column{Value: &shim.Column_String_{String_: lastUpdated}},
			&shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Error in adding KYC record.")
	}
	myLogger.Debug("Congratulations !!! Successfully added")
	return nil, err
}

/*
	Update KYC record
*/
func UpdateKYC(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Updating KYC record ...")

	if len(args) != 4 {
		return nil, errors.New("Incorrect number of arguments. Expecting 4")
	}

	customerId := args[0]
	kycStatus := args[1]
	lastUpdated := args[2]
	source := args[3]

	ok, err := stub.ReplaceRow("KYC", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			&shim.Column{Value: &shim.Column_String_{String_: kycStatus}},
			&shim.Column{Value: &shim.Column_String_{String_: lastUpdated}},
			&shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Error in updating KYC record.")
	}
	myLogger.Debug("Congratulations !!! Successfully updated")
	return nil, err
}

/*
 Get KYC record
*/
func GetKYC(stub *shim.ChaincodeStub, customerId string) (string, error) {
	var err error
	myLogger.Debugf("Get identification record for customer : [%s]", string(customerId))
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: customerId}}
	columns = append(columns, col1)
	row, err := stub.GetRow("KYC", columns)
	if err != nil {
		return "", fmt.Errorf("Failed retriving KYC details [%s]: [%s]", string(customerId), err)
	}
	jsonResp := "{\"customerId\":\"" + row.Columns[0].GetString_() + "\"" +
		",\"kycStatus\":\"" + row.Columns[1].GetString_() + "\"" +
		",\"lastUpdated\":\"" + row.Columns[2].GetString_() + "\"" +
		",\"source\":\"" + row.Columns[3].GetString_() + "\"}"

	return jsonResp, nil
}
