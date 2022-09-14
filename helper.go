package eshandler

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/elastic/go-ucfg"
	ucfgjson "github.com/elastic/go-ucfg/json"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func standartDiff(actual, expected any, log *logrus.Entry, ignore map[string]any) (diff string, err error) {
	acualByte, err := json.Marshal(actual)
	if err != nil {
		return diff, err
	}
	expectedByte, err := json.Marshal(expected)
	if err != nil {
		return diff, err
	}

	actualConf, err := ucfgjson.NewConfig(acualByte, ucfg.PathSep("."))
	if err != nil {
		log.Errorf("Error when converting current Json: %s\ndata: %s", err.Error(), string(acualByte))
		return diff, err
	}
	if err = ignoreDiff(actualConf, ignore); err != nil {
		return diff, err
	}
	actualUnpack := reflect.New(reflect.TypeOf(actual)).Interface()
	if err = actualConf.Unpack(actualUnpack, ucfg.StructTag("json")); err != nil {
		return diff, err
	}
	expectedConf, err := ucfgjson.NewConfig(expectedByte, ucfg.PathSep("."))
	if err != nil {
		log.Errorf("Error when converting new Json: %s\ndata: %s", err.Error(), string(expectedByte))
		return diff, err
	}
	if err = ignoreDiff(expectedConf, ignore); err != nil {
		return diff, err
	}
	expectedUnpack := reflect.New(reflect.TypeOf(expected)).Interface()
	if err = expectedConf.Unpack(expectedUnpack, ucfg.StructTag("json")); err != nil {
		return diff, err
	}

	test := map[string]any{}
	if err = expectedConf.Unpack(&test); err != nil {
		return diff, err
	}

	return cmp.Diff(actualUnpack, expectedUnpack), nil
}

func ignoreDiff(c *ucfg.Config, ignore map[string]any) (err error) {
		for key, value := range ignore {
			hasField, err := c.Has(key, -1, ucfg.PathSep("."))
			if err != nil {
				return err
			}
			if hasField {
				needRemoveKey := false
				if value == nil {
					needRemoveKey = true
				} else {
					var v any
					switch t := value.(type) {
					case bool:
						v, err = c.Bool(key, -1, ucfg.PathSep("."))
						if err != nil {
							return err
						}
					case string:
						v, err = c.String(key, -1, ucfg.PathSep("."))
						if err != nil {
							return err
						}
					case int64:
						v, err = c.Int(key, -1, ucfg.PathSep("."))
						if err != nil {
							return err
						}
					case float64:
						v, err = c.Float(key, -1, ucfg.PathSep("."))
						if err != nil {
							return err
						}
					default:
						return errors.Errorf("Type %T not supported", t)
					}

					if v == value {
						needRemoveKey = true
					}
				}
				if needRemoveKey {

					childPath := strings.Join(strings.Split(key, ".")[:1], ".")
					child, err := c.Child(childPath, -1, ucfg.PathSep("."))
					if err != nil {
						return err
					}
					if _, err = c.Remove(key, -1, ucfg.PathSep(".")); err != nil {
						return err
					}
					nb := len(child.GetFields())
					// Remove parent if no children
					if nb == 0 {
						if _, err = c.Remove(childPath, -1, ucfg.PathSep(".")); err != nil {
							return err
						}
					}

				}
			}
		}
	

	return nil
}
