// Code generated by entc, DO NOT EDIT.

package ent

import (
	"fmt"
	"strings"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/oceanbase/configserver/ent/obcluster"
)

// ObCluster is the model entity for the ObCluster schema.
type ObCluster struct {
	config `json:"-"`
	// ID of the ent.
	ID int `json:"id,omitempty"`
	// CreateTime holds the value of the "create_time" field.
	CreateTime time.Time `json:"create_time,omitempty"`
	// UpdateTime holds the value of the "update_time" field.
	UpdateTime time.Time `json:"update_time,omitempty"`
	// Name holds the value of the "name" field.
	Name string `json:"name,omitempty"`
	// ObClusterID holds the value of the "ob_cluster_id" field.
	ObClusterID int64 `json:"ob_cluster_id,omitempty"`
	// Type holds the value of the "type" field.
	Type string `json:"type,omitempty"`
	// RootserviceJSON holds the value of the "rootservice_json" field.
	RootserviceJSON string `json:"rootservice_json,omitempty"`
}

// scanValues returns the types for scanning values from sql.Rows.
func (*ObCluster) scanValues(columns []string) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	for i := range columns {
		switch columns[i] {
		case obcluster.FieldID, obcluster.FieldObClusterID:
			values[i] = new(sql.NullInt64)
		case obcluster.FieldName, obcluster.FieldType, obcluster.FieldRootserviceJSON:
			values[i] = new(sql.NullString)
		case obcluster.FieldCreateTime, obcluster.FieldUpdateTime:
			values[i] = new(sql.NullTime)
		default:
			return nil, fmt.Errorf("unexpected column %q for type ObCluster", columns[i])
		}
	}
	return values, nil
}

// assignValues assigns the values that were returned from sql.Rows (after scanning)
// to the ObCluster fields.
func (oc *ObCluster) assignValues(columns []string, values []interface{}) error {
	if m, n := len(values), len(columns); m < n {
		return fmt.Errorf("mismatch number of scan values: %d != %d", m, n)
	}
	for i := range columns {
		switch columns[i] {
		case obcluster.FieldID:
			value, ok := values[i].(*sql.NullInt64)
			if !ok {
				return fmt.Errorf("unexpected type %T for field id", value)
			}
			oc.ID = int(value.Int64)
		case obcluster.FieldCreateTime:
			if value, ok := values[i].(*sql.NullTime); !ok {
				return fmt.Errorf("unexpected type %T for field create_time", values[i])
			} else if value.Valid {
				oc.CreateTime = value.Time
			}
		case obcluster.FieldUpdateTime:
			if value, ok := values[i].(*sql.NullTime); !ok {
				return fmt.Errorf("unexpected type %T for field update_time", values[i])
			} else if value.Valid {
				oc.UpdateTime = value.Time
			}
		case obcluster.FieldName:
			if value, ok := values[i].(*sql.NullString); !ok {
				return fmt.Errorf("unexpected type %T for field name", values[i])
			} else if value.Valid {
				oc.Name = value.String
			}
		case obcluster.FieldObClusterID:
			if value, ok := values[i].(*sql.NullInt64); !ok {
				return fmt.Errorf("unexpected type %T for field ob_cluster_id", values[i])
			} else if value.Valid {
				oc.ObClusterID = value.Int64
			}
		case obcluster.FieldType:
			if value, ok := values[i].(*sql.NullString); !ok {
				return fmt.Errorf("unexpected type %T for field type", values[i])
			} else if value.Valid {
				oc.Type = value.String
			}
		case obcluster.FieldRootserviceJSON:
			if value, ok := values[i].(*sql.NullString); !ok {
				return fmt.Errorf("unexpected type %T for field rootservice_json", values[i])
			} else if value.Valid {
				oc.RootserviceJSON = value.String
			}
		}
	}
	return nil
}

// Update returns a builder for updating this ObCluster.
// Note that you need to call ObCluster.Unwrap() before calling this method if this ObCluster
// was returned from a transaction, and the transaction was committed or rolled back.
func (oc *ObCluster) Update() *ObClusterUpdateOne {
	return (&ObClusterClient{config: oc.config}).UpdateOne(oc)
}

// Unwrap unwraps the ObCluster entity that was returned from a transaction after it was closed,
// so that all future queries will be executed through the driver which created the transaction.
func (oc *ObCluster) Unwrap() *ObCluster {
	tx, ok := oc.config.driver.(*txDriver)
	if !ok {
		panic("ent: ObCluster is not a transactional entity")
	}
	oc.config.driver = tx.drv
	return oc
}

// String implements the fmt.Stringer.
func (oc *ObCluster) String() string {
	var builder strings.Builder
	builder.WriteString("ObCluster(")
	builder.WriteString(fmt.Sprintf("id=%v", oc.ID))
	builder.WriteString(", create_time=")
	builder.WriteString(oc.CreateTime.Format(time.ANSIC))
	builder.WriteString(", update_time=")
	builder.WriteString(oc.UpdateTime.Format(time.ANSIC))
	builder.WriteString(", name=")
	builder.WriteString(oc.Name)
	builder.WriteString(", ob_cluster_id=")
	builder.WriteString(fmt.Sprintf("%v", oc.ObClusterID))
	builder.WriteString(", type=")
	builder.WriteString(oc.Type)
	builder.WriteString(", rootservice_json=")
	builder.WriteString(oc.RootserviceJSON)
	builder.WriteByte(')')
	return builder.String()
}

// ObClusters is a parsable slice of ObCluster.
type ObClusters []*ObCluster

func (oc ObClusters) config(cfg config) {
	for _i := range oc {
		oc[_i].config = cfg
	}
}
