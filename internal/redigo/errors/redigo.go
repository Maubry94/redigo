package errors

import "errors"

var ErrorKeyNotFound = errors.New("key.notFound")
var ErrorKeyExpired = errors.New("key.expired")
var ErrorKeyAlreadyExists = errors.New("key.alreadyExists")
var ErrorUnsupportedValueType = errors.New("unsupported.valueType")
