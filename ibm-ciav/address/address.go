/*
Copyright IBM Corp. 2016 All Rights Reserved.
Licensed under the IBM India Pvt Ltd, Version 1.0 (the "License");
*/

package address

import (
	"errors"
	"fmt"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/op/go-logging"
  "bytes"
)

var myLogger = logging.MustGetLogger("customer_address_details")

/*
	Create address table
*/
func CreateTable(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Creating Address Table ...")
	if len(args) != 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0")
	}

	err := stub.CreateTable("Address", []*shim.ColumnDefinition{
    &shim.ColumnDefinition{Name: "customer_id", Type: shim.ColumnDefinition_STRING, Key: true},
		&shim.ColumnDefinition{Name: "address_id", Type: shim.ColumnDefinition_STRING, Key: true},
    &shim.ColumnDefinition{Name: "address_type", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "door_tumber", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "street", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "locality", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "city", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "state", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "pincode", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "poa_type", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "poa_doc", Type: shim.ColumnDefinition_STRING, Key: false},
		&shim.ColumnDefinition{Name: "expiry_date", Type: shim.ColumnDefinition_STRING, Key: false},
    &shim.ColumnDefinition{Name: "source", Type: shim.ColumnDefinition_STRING, Key: false},
	})
	if err != nil {
		return nil, errors.New("Failed creating Address table.")
	}

	myLogger.Debug("Address table created Successfully... !!! ")
	return nil, nil
}

/*
	Add Address record
*/
func AddAddress(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Adding Address record ...")

	if len(args) != 13 {
		return nil, errors.New("Incorrect number of arguments. Expecting 13")
	}
    customerId := args[0]
    addressId := args[1]
  	addressType := args[2]
  	doorNumber := args[3]
  	street := args[4]
  	locality := args[5]
		city := args[6]
  	state := args[7]
  	pincode := args[8]
  	poaType := args[9]
  	poaDoc := args[10]
  	expiryDate := args[11]
  	source := args[12]

	ok, err := stub.InsertRow("Address", shim.Row{
		Columns: []*shim.Column{
      &shim.Column{Value: &shim.Column_String_{String_: customerId}},
			&shim.Column{Value: &shim.Column_String_{String_: addressId}},
			&shim.Column{Value: &shim.Column_String_{String_: addressType}},
			&shim.Column{Value: &shim.Column_String_{String_: doorNumber}},
			&shim.Column{Value: &shim.Column_String_{String_: street}},
			&shim.Column{Value: &shim.Column_String_{String_: locality}},
			&shim.Column{Value: &shim.Column_String_{String_: city}},
			&shim.Column{Value: &shim.Column_String_{String_: state}},
			&shim.Column{Value: &shim.Column_String_{String_: pincode}},
			&shim.Column{Value: &shim.Column_String_{String_: poaType}},
			&shim.Column{Value: &shim.Column_String_{String_: poaDoc}},
      &shim.Column{Value: &shim.Column_String_{String_: expiryDate}},
      &shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Error in adding address record.")
	}
	myLogger.Debug("Congratulations !!! Successfully added",)
	return nil, err
}

/*
	Update address record
*/
func UpdateAddress(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
	myLogger.Debug("Updating address record ...")

	if len(args) != 11 {
		return nil, errors.New("Incorrect number of arguments. Expecting 11")
	}

  customerId := args[0]
  addressId := args[1]
  addressType := args[2]
  doorNumber := args[3]
  street := args[4]
  locality := args[5]
  city := args[6]
  state := args[7]
  pincode := args[8]
  poaType := args[9]
  poaDoc := args[10]
  expiryDate := args[11]
  source := args[12]

	ok, err := stub.ReplaceRow("Address", shim.Row{
		Columns: []*shim.Column{
      &shim.Column{Value: &shim.Column_String_{String_: customerId}},
      &shim.Column{Value: &shim.Column_String_{String_: addressId}},
      &shim.Column{Value: &shim.Column_String_{String_: addressType}},
      &shim.Column{Value: &shim.Column_String_{String_: doorNumber}},
      &shim.Column{Value: &shim.Column_String_{String_: street}},
      &shim.Column{Value: &shim.Column_String_{String_: locality}},
			&shim.Column{Value: &shim.Column_String_{String_: city}},
      &shim.Column{Value: &shim.Column_String_{String_: state}},
      &shim.Column{Value: &shim.Column_String_{String_: pincode}},
      &shim.Column{Value: &shim.Column_String_{String_: poaType}},
      &shim.Column{Value: &shim.Column_String_{String_: poaDoc}},
      &shim.Column{Value: &shim.Column_String_{String_: expiryDate}},
      &shim.Column{Value: &shim.Column_String_{String_: source}},
		},
	})

	if !ok && err == nil {
		return nil, errors.New("Error in updated customer address record.")
	}
	myLogger.Debug("Congratulations !!! Successfully updated ",)
	return nil, err
}

/*
 Get address record
*/
func GetAddress(stub *shim.ChaincodeStub, customerId string) (string, error) {
	var err error
	myLogger.Debugf("Getting address record for customer : [%s]", string(customerId))

	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: customerId}}
	columns = append(columns, col1)
	rows, err := GetAllRows(stub, "Address", columns)
	if err != nil {
		myLogger.Debugf("Failed retriving Address details [%s]: [%s]", string(customerId), err)
		return "", fmt.Errorf("Failed retriving Address details [%s]: [%s]", string(customerId), err)
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
			",\"addressId\":\"" + row.Columns[1].GetString_() + "\"" +
			",\"addressType\":\"" + row.Columns[2].GetString_() + "\"" +
			",\"doorNumber\":\"" + row.Columns[3].GetString_() + "\"" +
			",\"street\":\"" + row.Columns[4].GetString_() + "\"" +
			",\"locality\":\"" + row.Columns[5].GetString_() + "\"" +
			",\"city\":\"" + row.Columns[6].GetString_() + "\"" +
			",\"state\":\"" + row.Columns[7].GetString_() + "\"" +
			",\"pincode\":\"" + row.Columns[8].GetString_() + "\"" +
			",\"poaType\":\"" + row.Columns[9].GetString_() + "\"" +
			",\"poaDoc\":\"" + row.Columns[10].GetString_() + "\"" +
	    ",\"expiryDate\":\"" + row.Columns[11].GetString_() + "\"" +
	    ",\"source\":\"" + row.Columns[12].GetString_() + "\"}")
	}
	jsonRespBuffer.WriteString("]")

	return jsonRespBuffer.String(), nil
}

/*
	Get all rows corresponding to the partial keys given
*/
func GetAllRows(stub *shim.ChaincodeStub, tableName string, columns []shim.Column) ([]shim.Row, error) {
	rowChannel, err := stub.GetRows(tableName, columns)
	if err != nil {
		myLogger.Debugf("Failed retriving address details for : [%s]", err)
		return nil, fmt.Errorf("Failed retriving address details : [%s]", err)
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
