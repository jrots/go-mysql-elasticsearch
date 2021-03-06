package river

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"
	"encoding/json"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/jrots/go-mysql-elasticsearch/elasticwrapper"
	"github.com/jrots/go-mysql/canal"
	"github.com/jrots/go-mysql/mysql"
	"github.com/jrots/go-mysql/replication"
	"github.com/jrots/go-mysql/schema"
)

const (
	syncInsertDoc = iota
	syncDeleteDoc
	syncUpdateDoc
)

const (
	fieldTypeList = "list"
	fieldTypeGeoLat = "geo_lat"
	fieldTypeGeoLon = "geo_lon"
	fieldTypeNumericBool = "numeric_bool"

)

type posSaver struct {
	pos   mysql.Position
	force bool
}

type eventHandler struct {
	r *River
}

func (h *eventHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		string(e.NextLogName),
		uint32(e.Position),
	}

	h.r.syncCh <- posSaver{pos, true}

	return h.r.ctx.Err()
}

func (h *eventHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.r.syncCh <- posSaver{nextPos, true}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnXID(nextPos mysql.Position) error {
	h.r.syncCh <- posSaver{nextPos, false}
	return h.r.ctx.Err()
}

func (h *eventHandler) OnRow(e *canal.RowsEvent) error {
	rule, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}
	var reqs []*elasticwrapper.BulkRequest
	var err error
	switch e.Action {
	case canal.InsertAction:
		reqs, err = h.r.makeInsertRequest(rule, e.Rows)
	case canal.DeleteAction:
		reqs, err = h.r.makeDeleteRequest(rule, e.Rows)
	case canal.UpdateAction:
		reqs, err = h.r.makeUpdateRequest(rule, e.Rows)
	default:
		err = errors.Errorf("invalid rows action %s", e.Action)
	}

	if err != nil {
		h.r.cancel()
		return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
	}

	h.r.syncCh <- reqs

	return h.r.ctx.Err()
}

func (h *eventHandler) String() string {
	return "ESRiverEventHandler"
}

func (r *River) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime.Duration
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]*elasticwrapper.BulkRequest, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []*elasticwrapper.BulkRequest:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.doBulk(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}

// for insert and delete
func (r *River) makeRequest(rule *Rule, action string, rows [][]interface{}) ([]*elasticwrapper.BulkRequest, error) {
	reqs := make([]*elasticwrapper.BulkRequest, 0, len(rows))

	for _, values := range rows {
		id, err := r.getDocID(rule, values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		parentID := ""
		if len(rule.Parent) > 0 {
			if parentID, err = r.getParentID(rule, values, rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}

		req := &elasticwrapper.BulkRequest{Index: rule.Index, Type: rule.Type, ID: id, Parent: parentID}

		if len(rule.IdPrefix) > 0 {
			req.ID = rule.IdPrefix + ":" + req.ID
		}

		if rule.ConcatField != "" {
			req.ListRequest = true
		}

		if len(rule.JoinField) > 0 {
			req.JoinField = rule.JoinField
		}
		if len(rule.JoinFieldName) > 0 {
			req.JoinFieldName = rule.JoinFieldName
		}
		req.HardCrud = rule.HardCrud

		if action == canal.DeleteAction {
			if !rule.HardCrud {
				r.makeInsertReqData(req, rule, values)
			}
			req.Action = elasticwrapper.ActionDelete
			r.st.DeleteNum.Add(1)
		} else {
			r.makeInsertReqData(req, rule, values)
			if rule.HardCrud {
				req.Action = elasticwrapper.ActionIndex
			} else {
				req.Action = elasticwrapper.ActionUpdate //upsert in this case (don't override complete document with the data)
			}
			req.Initial = true
			r.st.InsertNum.Add(1)
		}

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeInsertRequest(rule *Rule, rows [][]interface{}) ([]*elasticwrapper.BulkRequest, error) {
	return r.makeRequest(rule, canal.InsertAction, rows)
}

func (r *River) makeDeleteRequest(rule *Rule, rows [][]interface{}) ([]*elasticwrapper.BulkRequest, error) {
	return r.makeRequest(rule, canal.DeleteAction, rows)
}

func (r *River) makeUpdateRequest(rule *Rule, rows [][]interface{}) ([]*elasticwrapper.BulkRequest, error) {
	if len(rows)%2 != 0 {
		return nil, errors.Errorf("invalid update rows event, must have 2x rows, but %d", len(rows))
	}

	reqs := make([]*elasticwrapper.BulkRequest, 0, len(rows))

	for i := 0; i < len(rows); i += 2 {
		beforeID, err := r.getDocID(rule, rows[i])
		if err != nil {
			return nil, errors.Trace(err)
		}

		beforeParentID := ""
		if len(rule.Parent) > 0 {
			if beforeParentID, err = r.getParentID(rule, rows[i], rule.Parent); err != nil {
				return nil, errors.Trace(err)
			}
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Simplify .. no support for changing PK of rows as this would complicate things too much
		req := &elasticwrapper.BulkRequest{Index: rule.Index, Type: rule.Type, ID: beforeID, Parent: beforeParentID, HardCrud: rule.HardCrud}

		if rule.ConcatField != "" {
			req.ListRequest = true
		}

		if len(rule.IdPrefix) > 0 {
			req.ID = rule.IdPrefix + ":" + req.ID
		}

		if len(rule.JoinField) > 0 {
			req.JoinField = rule.JoinField
		}
		if len(rule.JoinFieldName) > 0 {
			req.JoinFieldName = rule.JoinFieldName
		}

		r.makeUpdateReqData(req, rule, rows[i], rows[i+1])
		r.st.UpdateNum.Add(1)

		reqs = append(reqs, req)
	}

	return reqs, nil
}

func (r *River) makeReqColumnData(col *schema.TableColumn, value interface{}) interface{} {
	switch col.Type {
	case schema.TYPE_ENUM:
		switch value := value.(type) {
		case int64:
			// for binlog, ENUM may be int64, but for dump, enum is string
			eNum := value - 1
			if eNum < 0 || eNum >= int64(len(col.EnumValues)) {
				// we insert invalid enum value before, so return empty
				log.Warnf("invalid binlog enum index %d, for enum %v", eNum, col.EnumValues)
				return ""
			}

			return col.EnumValues[eNum]
		}
	case schema.TYPE_SET:
		switch value := value.(type) {
		case int64:
			// for binlog, SET may be int64, but for dump, SET is string
			bitmask := value
			sets := make([]string, 0, len(col.SetValues))
			for i, s := range col.SetValues {
				if bitmask&int64(1<<uint(i)) > 0 {
					sets = append(sets, s)
				}
			}
			return strings.Join(sets, ",")
		}
	case schema.TYPE_BIT:
		switch value := value.(type) {
		case string:
			// for binlog, BIT is int64, but for dump, BIT is string
			// for dump 0x01 is for 1, \0 is for 0
			if value == "\x01" {
				return int64(1)
			}

			return int64(0)
		}
	case schema.TYPE_STRING:
		switch value := value.(type) {
		case []byte:
			return string(value[:])
		}
	case schema.TYPE_DATETIME:
	case schema.TYPE_TIMESTAMP:
		var stringVal string
		switch value := value.(type) {
		case []byte:
			stringVal = string(value[:])
		case string:
			stringVal = value
		default:
			return value
		}
		if stringVal == "0000-00-00 00:00:00" || stringVal == "1970-01-01 01:00:00" || stringVal == "1970-01-01 00:00:00" {
			return nil
 		} else {
			return stringVal
		}
	case schema.TYPE_DATE:
		var stringVal string
		switch value := value.(type) {
		case []byte:
			stringVal = string(value[:])
		case string:
			stringVal = value
		default:
			return value
		}
		if stringVal == "0000-00-00" {
			return nil
 		}
		return stringVal
	case schema.TYPE_JSON:
		var f interface{}
		var err error
		switch v := value.(type) {
		case string:
			err = json.Unmarshal([]byte(v), &f)
		case []byte:
			err = json.Unmarshal(v, &f)
		}
		if err == nil && f != nil {
			return f
		}
	}

	return value
}

func (r *River) getFieldParts(k string, v string) (string, string, string) {
	composedField := strings.Split(v, ",")

	mysql := k
	elasticwrapper := composedField[0]
	fieldType := ""

	if 0 == len(elasticwrapper) {
		elasticwrapper = mysql
	}
	if 2 == len(composedField) {
		fieldType = composedField[1]
	}

	return mysql, elasticwrapper, fieldType
}

func (r *River) makeInsertReqData(req *elasticwrapper.BulkRequest, rule *Rule, values []interface{}) {
	req.Data = make(map[string]interface{}, len(values))
	if !rule.HardCrud {
		req.Action = elasticwrapper.ActionUpdate
	}
	concatField := bytes.NewBufferString("")
	if rule.ConcatField != "" {
		concatField.WriteString(rule.ConcatPrefix)
	}

	for i, c := range rule.TableInfo.Columns {
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if i >= len(values) {
			continue
		}
		mapped := false
		for k, v := range rule.FieldMapping {
			mysql, elasticwrapper, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				v := r.makeReqColumnData(&c, values[i])
				if v == nil {
					continue
				}
				if fieldType == fieldTypeList {
					if str, ok := v.(string); ok {
						req.Data[elasticwrapper] = strings.Split(str, ",")
					} else {
						req.Data[elasticwrapper] = v
					}
				} else if fieldType == fieldTypeNumericBool {
					boolVal, ok := v.(int64)
					req.Data[elasticwrapper] = 0
					if ok && boolVal > 0 {
						req.Data[elasticwrapper] = 1
					}
				} else if fieldType == fieldTypeGeoLat || fieldType == fieldTypeGeoLon {
					if _, ok := req.Data[elasticwrapper]; !ok {
						req.Data[elasticwrapper] = make(map[string]interface{})
					}
					md, ok := req.Data[elasticwrapper].(map[string]interface{})
					if (ok){
						if (fieldType == fieldTypeGeoLat) {
							md["lat"] = v
						} else {
							md["lon"] = v
						}
						req.Data[elasticwrapper] = md
					}
				} else {
					req.Data[elasticwrapper] = v
				}
			}
		}
		if mapped == false {
			v := r.makeReqColumnData(&c, values[i])
			if v != nil {
				if rule.ConcatField != "" {
					concatField.WriteString("_")
					concatField.WriteString(fmt.Sprint(v))
				} else {
					req.Data[c.Name] = v
				}
			}
		}
	}
	if rule.ConcatField != "" {
		req.Data[rule.ConcatField] = concatField.String()
	}

}

func (r *River) makeUpdateReqData(req *elasticwrapper.BulkRequest, rule *Rule,
	beforeValues []interface{}, afterValues []interface{}) {
	req.Data = make(map[string]interface{}, len(beforeValues))
	req.DeleteFields = make(map[string]interface{}, len(beforeValues))

	// maybe dangerous if something wrong delete before?
	req.Action = elasticwrapper.ActionUpdate
	concatField := bytes.NewBufferString("")
	if rule.ConcatField != "" {
		concatField.WriteString(rule.ConcatPrefix)
	}

	for i, c := range rule.TableInfo.Columns {
		mapped := false
		if !rule.CheckFilter(c.Name) {
			continue
		}
		if i >= len(beforeValues) || i >= len(afterValues) {
			continue
		}
		if reflect.DeepEqual(beforeValues[i], afterValues[i]) {
			//nothing changed
			continue
		}
		for k, v := range rule.FieldMapping {
			mysql, elasticwrapper, fieldType := r.getFieldParts(k, v)
			if mysql == c.Name {
				mapped = true
				// has custom field mapping
				v := r.makeReqColumnData(&c, afterValues[i])

				if v == nil {
					req.DeleteFields[elasticwrapper] = true
					continue
				}
				str, ok := v.(string)
				if fieldType == fieldTypeNumericBool {
					boolVal, ok := v.(int32)
					req.Data[elasticwrapper] = 0
					if ok && boolVal > 0 {
						req.Data[elasticwrapper] = 1
					}
				} else if fieldType == fieldTypeGeoLat || fieldType == fieldTypeGeoLon {
					if _, ok := req.Data[elasticwrapper]; !ok {
						req.Data[elasticwrapper] = make(map[string]interface{})
					}
					md, ok := req.Data[elasticwrapper].(map[string]interface{})
					if ok {
						if fieldType == fieldTypeGeoLat {
							md["lat"] = v
						} else {
							md["lon"] = v
						}
						req.Data[elasticwrapper] = md
					}
				} else if ok == false {
					req.Data[elasticwrapper] = v
				} else {
					if fieldType == fieldTypeList {
						req.Data[elasticwrapper] = strings.Split(str, ",")
					} else {
						req.Data[elasticwrapper] = str
					}
				}
			}
		}
		if mapped == false {
			v := r.makeReqColumnData(&c, afterValues[i])

			if v == nil {
				req.DeleteFields[c.Name] = true
			} else {
				if rule.ConcatField != "" {
					concatField.WriteString("_")
					concatField.WriteString(fmt.Sprint(v))
				} else {
					req.Data[c.Name] = v
				}
			}
		}
	}
	if rule.ConcatField != "" {
		req.Data[rule.ConcatField] = concatField.String()
	}
}

// If id in toml file is none, get primary keys in one row and format them into a string, and PK must not be nil
// Else get the ID's column in one row and format them into a string
func (r *River) getDocID(rule *Rule, row []interface{}) (string, error) {
	var (
  		ids []interface{}
  		err error 
	)
	if rule.ID == nil {
		ids, err = canal.GetPKValues(rule.TableInfo, row)
		if err != nil {
			return "", err
		}
	} else {
		ids = make([]interface{}, 0, len(rule.ID))
		for _, column := range rule.ID {
			value, err := canal.GetColumnValue(rule.TableInfo, column, row)
			if err != nil {
				return "", err
			}
			ids = append(ids, value)
		}
	}

	var buf bytes.Buffer

	sep := ""
	for i, value := range ids {
		if value == nil {
			return "", errors.Errorf("The %ds id or PK value is nil", i)
		}

		buf.WriteString(fmt.Sprintf("%s%v", sep, value))
		sep = ":"
	}

	return buf.String(), nil
}

func (r *River) getParentID(rule *Rule, row []interface{}, columnName string) (string, error) {
	index := rule.TableInfo.FindColumn(columnName)
	if index < 0 {
		return "", errors.Errorf("parent id not found %s(%s)", rule.TableInfo.Name, columnName)
	}

	return fmt.Sprint(row[index]), nil
}

func (r *River) doBulk(reqs []*elasticwrapper.BulkRequest) error {
	if len(reqs) == 0 {
		return nil
	}

	if resp, err := r.es.Bulk(reqs); err != nil {
		log.Errorf("sync docs err %v after binlog %s", err, r.canal.SyncedPosition())
		return errors.Trace(err)
	} else if resp.Code / 100 == 2 || resp.Errors {
		for i := 0; i < len(resp.Items); i++ {
			for action, item := range resp.Items[i] {
				if len(item.Error) > 0 {
					log.Errorf("%s index: %s, type: %s, id: %s, status: %d, error: %s",
						action, item.Index, item.Type, item.ID, item.Status, item.Error)
				}
			}
		}
	}

	return nil
}
