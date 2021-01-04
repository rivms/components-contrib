// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package digitaltwins

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"

	"github.com/Azure/go-autorest/autorest/azure/auth"

	"github.com/dapr/components-contrib/bindings/azure/digitaltwins/digitaltwinsrest"
)

const (
	key = "partitionKey"
)

// AzureDigitalTwins allows writing to a Azure Digital Twins instance
type AzureDigitalTwins struct {
	clientID       string
	clientSecret   string
	tenantID       string
	adtInstanceURL string
	logger         logger.Logger
}

type azureDigitalTwinsMetadata struct {
	clientID       string `json:"clientId"`
	clientSecret   string `json:"clientSecret"`
	tenantID       string `json:"tenantId"`
	adtInstanceURL string `json:"adtInstanceUrl"`
}

type jsonPatchOperation struct {
	Op     string      `json:"op"`
	Path   string      `json:"path"`
	Value  interface{} `json:"value,omitempty"`
	TwinID string      `json:"-"`
}

// NewAzureDigitalTwins returns a new Azure Digital Twins binding instance
func NewAzureDigitalTwins(logger logger.Logger) *AzureDigitalTwins {
	return &AzureDigitalTwins{logger: logger}
}

// Init does metadata parsing and connection establishment
func (d *AzureDigitalTwins) Init(metadata bindings.Metadata) error {

	d.logger.Infof("Init invoked...Azure Digital Twins")
	meta, err := d.getAzureDigitalTwinsMetadata(metadata)
	if err != nil {
		return err
	}

	d.clientID = meta.clientID
	d.clientSecret = meta.clientSecret
	d.tenantID = meta.tenantID
	d.adtInstanceURL = meta.adtInstanceURL

	return nil
}

func (d *AzureDigitalTwins) patchSingleTwin(twinID string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	d.logger.Debugf("Patching single twin")
	var operationDoc []jsonPatchOperation

	err := json.Unmarshal(req.Data, &operationDoc)

	if err != nil {
		d.logger.Errorf("Request data json error: %s", err)
		return nil, nil
	}

	ccc := auth.NewClientCredentialsConfig(d.clientID, d.clientSecret, d.tenantID)
	ccc.Resource = "https://digitaltwins.azure.net"

	client := digitaltwinsrest.NewDigitalTwinsClientWithBaseURI(d.adtInstanceURL)
	authorizer, _ := ccc.Authorizer()

	client.Authorizer = authorizer

	s := make([]interface{}, len(operationDoc))
	for i, v := range operationDoc {
		s[i] = v
	}

	client.Update(context.TODO(), twinID, s, "*", "", "")

	return nil, nil
}

func (d *AzureDigitalTwins) patchMultipleTwin(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var operationDoc []jsonPatchOperation

	err := json.Unmarshal(req.Data, &operationDoc)

	if err != nil {
		d.logger.Errorf("Request data json error: %s", err)
		return nil, nil
	}

	r, err := regexp.Compile("^/(.+?)\\/(.+)$")

	if err != nil {
		d.logger.Debugf("Regex compilation error: %s", err)
		return nil, nil
	}

	// First pass extracts twin id from patch operation path, fails entire request on error
	for i, v := range operationDoc {
		matches := r.FindStringSubmatch(v.Path)

		if len(matches) < 3 || len(matches) > 3 {
			d.logger.Errorf("Invalid path in patch: %s", v.Path)
			return nil, nil
		}

		operationDoc[i].TwinID = matches[1]
		operationDoc[i].Path = "/" + matches[2]

		// Invoke
	}

	// Second pass invokes digital twins api
	for i, v := range operationDoc {
		patchDoc := []interface{}{v}
		d.logger.Infof("[%d] Operation to submit to digital twin (%s): %s", i, v.TwinID, patchDoc)
		b, err := json.Marshal(patchDoc)
		if err != nil {
			d.logger.Errorf("Error marshalling operation doc: %s", err)
			return nil, nil
		}

		d.logger.Infof("Calling API for twin (%s) with patch: %s", v.TwinID, string(b))

		//d.patchTwin(v)

		ccc := auth.NewClientCredentialsConfig(d.clientID, d.clientSecret, d.tenantID)
		ccc.Resource = "https://digitaltwins.azure.net"

		client := digitaltwinsrest.NewDigitalTwinsClientWithBaseURI(d.adtInstanceURL)
		authorizer, _ := ccc.Authorizer()

		client.Authorizer = authorizer

		client.Update(context.TODO(), v.TwinID, patchDoc, "*", "", "")
	}

	return nil, nil
}

// Operations returns list of supported operations
func (*AzureDigitalTwins) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke executes output binding
// Expects twin id in path e.g., "path": "/myTwinId/property1"
func (d *AzureDigitalTwins) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {

	d.logger.Infof("Invoke called with data: %s", req.Data)
	d.logger.Infof("Invoke called with metadata: %s", req.Metadata)

	if val, ok := req.Metadata["twinID"]; ok && val != "" {
		d.logger.Infof("Metadata twinID: %s", val)
		response, err := d.patchSingleTwin(val, req)
		return response, err
	} else {
		d.logger.Infof("Metadata twinID not found.")
		response, err := d.patchMultipleTwin(req)
		return response, err
	}

	// var operationDoc []jsonPatchOperation

	// err := json.Unmarshal(req.Data, &operationDoc)

	// if err != nil {
	//	d.logger.Errorf("Request data json error: %s", err)
	//	return nil, nil
	// }

	// r, err := regexp.Compile("^/(.+?)\\/(.+)$")

	// if err != nil {
	//	d.logger.Debugf("Regex compilation error: %s", err)
	//	return nil, nil
	// }

	// First pass extracts twin id from patch operation path, fails entire request on error
	// for i, v := range operationDoc {
	// 	matches := r.FindStringSubmatch(v.Path)

	//	if len(matches) < 3 || len(matches) > 3 {
	//		d.logger.Errorf("Invalid path in patch: %s", v.Path)
	//		return nil, nil
	//	}

	//	operationDoc[i].TwinID = matches[1]
	//	operationDoc[i].Path = "/" + matches[2]

	// Invoke
	// }

	// Second pass invokes digital twins api
	// for i, v := range operationDoc {
	//	patchDoc := []interface{}{v}
	//	d.logger.Infof("[%d] Operation to submit to digital twin (%s): %s", i, v.TwinID, patchDoc)
	//	b, err := json.Marshal(patchDoc)
	//	if err != nil {
	//		d.logger.Errorf("Error marshalling operation doc: %s", err)
	//		return nil, nil
	//	}

	//	d.logger.Infof("Calling API for twin (%s) with patch: %s", v.TwinID, string(b))
	//	d.patchTwin(v)
	//}

	return nil, nil
}

/*
func (d *AzureDigitalTwins) patchTwin(patchOp jsonPatchOperation) {
	ccc := auth.NewClientCredentialsConfig(d.clientID, d.clientSecret, d.tenantID)
	ccc.Resource = "https://digitaltwins.azure.net"

	client := NewDigitalTwinsClientWithBaseURI(d.adtInstanceURL)
	authorizer, _ := ccc.Authorizer()

	client.Authorizer = authorizer

	patchDoc := []interface{}{patchOp}

	client.Update(context.TODO(), patchOp.TwinID, patchDoc, "*", "", "")
}
*/

func (*AzureDigitalTwins) getAzureDigitalTwinsMetadata(metadata bindings.Metadata) (*azureDigitalTwinsMetadata, error) {
	meta := azureDigitalTwinsMetadata{}

	if val, ok := metadata.Properties["clientId"]; ok && val != "" {
		meta.clientID = val
	} else {
		return nil, errors.New("azureDigitalTwins error: missing clientId")
	}

	if val, ok := metadata.Properties["clientSecret"]; ok && val != "" {
		meta.clientSecret = val
	} else {
		return nil, errors.New("azureDigitalTwins error: missing clientSecret")
	}

	if val, ok := metadata.Properties["tenantId"]; ok && val != "" {
		meta.tenantID = val
	} else {
		return nil, errors.New("azureDigitalTwins error: missing tenantId")
	}

	if val, ok := metadata.Properties["adtInstanceUrl"]; ok && val != "" {
		meta.adtInstanceURL = val
	} else {
		return nil, errors.New("azureDigitalTwins error: missing adtInstanceUrl")
	}

	return &meta, nil
}
