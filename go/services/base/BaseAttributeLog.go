package base

import (
	"fmt"
	"reflect"
)

func PrintAttributeValue(elem interface{}, msg, typ, field string) {
	v := reflect.ValueOf(elem).Elem()
	if v.Type().Name() == typ {
		fld := v.FieldByName(field)
		if fld.IsValid() {
			fmt.Println(msg, "/", typ, "=", fld.String())
		}
	}
}
