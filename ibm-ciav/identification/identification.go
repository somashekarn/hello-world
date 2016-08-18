/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the IBM India Pvt Ltd, Version 1.0 (the "License");
*/

package identification

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
	"strings"
)

var myLogger = logging.MustGetLogger("customer_identification_details")

/*
 Create Identification table
*/
func CreateTable(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Init Identification Chaincode...")
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	// Create Identification table
	err := stub.CreateTable("Identification", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "customer_id", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "identity_number", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "poi_type", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "poi_doc", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "expiry_date", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "source", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	// Create identification relation table
	err = stub.CreateTable("IDRelation", []*shim.ColumnDefinition{
		&shim.ColumnDefinition{Name: "identity_number", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "customer_id", Type: shim.ColumnDefinition_STRING, Key: false},
	})

	if err != nil {
		return nil, errors.New("Failed creating Identification table.")
	}
	myLogger.Debug("Identification table initialization done Successfully... !!! ")
	return nil, nil
}

/*
	add Identification record
*/
func AddIdentification(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Add Identification record ...")
	if len(args) != 6 {
		return nil, errors.New("Incorrect number of arguments. Expecting 6")
	}

	customerId := args[0]
	identityNumber := args[1]
	poiType := args[2]
	poiDoc := args[3]
	expiryDate := args[4]
	source := args[5]

	myLogger.Debugf("Adding identity doc : [%s] ", poiType)

	ok, err := stub.InsertRow("Identification", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			&shim.Column{Value: &shim.Column_String_{String_: identityNumber}},
			&shim.Column{Value: &shim.Column_String_{String_: poiType}},
			&shim.Column{Value: &shim.Column_String_{String_: poiDoc}},
			&shim.Column{Value: &shim.Column_String_{String_: expiryDate}},
			&shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	// update identification relation
	res, err := updateIDRelation(stub, identityNumber, customerId, "add")
	if !ok && err == nil {
		return nil, errors.New("Error in adding Identification record.")
	}
	myLogger.Debug("Congratulations !!! Successfully added, [%s]", res)
	return nil, err
}

/*
 Update Identification record
*/
func UpdateIdentification(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Update Identification record ...")

	if len(args) != 6 {
		return nil, errors.New("Incorrect number of arguments. Expecting 6")
	}

	customerId := args[0]
	identityNumber := args[1]
	poiType := args[2]
	poiDoc := args[3]
	expiryDate := args[4]
	source := args[5]

	myLogger.Debugf("Updating identity : [%s] ", poiType)

	ok, err := stub.ReplaceRow("Identification", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			&shim.Column{Value: &shim.Column_String_{String_: identityNumber}},
			&shim.Column{Value: &shim.Column_String_{String_: poiType}},
			&shim.Column{Value: &shim.Column_String_{String_: poiDoc}},
			&shim.Column{Value: &shim.Column_String_{String_: expiryDate}},
			&shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	res, err := updateIDRelation(stub, identityNumber, customerId, "update")
	ok, err = stub.ReplaceRow("IDRelation", shim.Row{
		Columns: []*shim.Column{
			&shim.Column{Value: &shim.Column_String_{String_: identityNumber}},
			&shim.Column{Value: &shim.Column_String_{String_: customerId}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Error in updating Identification record.")
	}
	myLogger.Debug("Congratulations !!! Successfully updated [%s]", res)
	return nil, err
}

/*
	Get Identification record
*/
func GetIdentification(stub *shim.ChaincodeStub, customerId string) (string, error) {
	var err error
	myLogger.Debugf("Get identification record for customer : [%s]", string(customerId))

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: customerId}}
	columns = append(columns, col1)
	rows, err := GetAllRows(stub, "Identification", columns)
	if err != nil {
		myLogger.Debugf("Failed retriving Identification details [%s]: [%s]", string(customerId), err)
		return "", fmt.Errorf("Failed retriving Identification details [%s]: [%s]", string(customerId), err)
	}

	var jsonRespBuffer bytes.Buffer
	jsonRespBuffer.WriteString("[")
	for i := range rows {
		row := rows[i]
		myLogger.Debugf("Identification rows [%s], is : [%s]", i, row)
		fmt.Println(row)
		if i != 0 {
			jsonRespBuffer.WriteString(",")
		}
		jsonRespBuffer.WriteString("{\"customerId\":\"" + row.Columns[0].GetString_() + "\"" +
			",\"identityNumber\":\"" + row.Columns[1].GetString_() + "\"" +
			",\"poiType\":\"" + row.Columns[2].GetString_() + "\"" +
			",\"poiDoc\":\"" + row.Columns[3].GetString_() + "\"" +
			",\"expiryDate\":\"" + row.Columns[4].GetString_() + "\"" +
			",\"source\":\"" + row.Columns[5].GetString_() + "\"}")
	}
	jsonRespBuffer.WriteString("]")

	return jsonRespBuffer.String(), nil
}

/*
 Get the customer id by PAN number
*/
func GetCustomerID(stub *shim.ChaincodeStub, panId string) ([]string, error) {
	var err error

	myLogger.Debugf("Get customer id for PAN : [%s]", panId)

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: panId}}
	columns = append(columns, col1)

	row, err := stub.GetRow("IDRelation", columns)
	if err != nil {
		myLogger.Debugf("Failed retriving Identification details for PAN [%s]: [%s]", string(panId), err)
		return nil, fmt.Errorf("Failed retriving Identification details  for PAN [%s]: [%s]", string(panId), err)
	}

	custIds := row.Columns[1].GetString_()
	custIdArray := strings.Split(custIds, "|")
	return custIdArray, nil
}

/*
	Get all rows corresponding to the partial keys given
*/
func GetAllRows(stub *shim.ChaincodeStub, tableName string, columns []shim.Column) ([]shim.Row, error) {
	rowChannel, err := stub.GetRows(tableName, columns)
	if err != nil {
		myLogger.Debugf("Failed retriving Identification details for : [%s]", err)
		return nil, fmt.Errorf("Failed retriving Identification details : [%s]", err)
	}

	var rows []shim.Row
	for {
		select {
		case temprow, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				myLogger.Debugf("Fetching row : [%s]", temprow.Columns[0].GetString_())
				rows = append(rows, temprow)
			}
		}
		if rowChannel == nil {
			break
		}
	}
	return rows, nil
}

/*
	Update ID relation table
*/
func updateIDRelation(stub *shim.ChaincodeStub, identityNumber string, customerId string, functionType string) (string, error) {
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: identityNumber}}
	columns = append(columns, col1)

	idrow, err := stub.GetRow("IDRelation", columns)
	if err != nil {
		myLogger.Debugf("Failed retriving Identification relation details for ID [%s]: [%s]", string(identityNumber), err)
		return "", fmt.Errorf("Failed retriving Identification relation details  for ID [%s]: [%s]", string(identityNumber), err)
	}

	var isRowExists bool
	isRowExists = (idrow.Columns != nil)

	var ok bool
	if isRowExists {
		if functionType == "update" {
			if strings.Contains(idrow.Columns[1].GetString_(), customerId) {
				myLogger.Debugf("Identification relation exists. Do nothing.")
				return "", nil
			}
		}
		customerId = idrow.Columns[1].GetString_() + "|" + customerId
		ok, err = stub.ReplaceRow("IDRelation", shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: identityNumber}},
				&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			},
		})
	} else {
		ok, err = stub.InsertRow("IDRelation", shim.Row{
			Columns: []*shim.Column{
				&shim.Column{Value: &shim.Column_String_{String_: identityNumber}},
				&shim.Column{Value: &shim.Column_String_{String_: customerId}},
			},
		})
	}
	if !ok && err == nil {
		return "", errors.New("Error in updating Identification relation record.")
	}
	return "", nil
}
