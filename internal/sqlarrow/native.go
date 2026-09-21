package sqlarrow

type floats interface {
	float32 | float64
}

type signedIntegers interface {
	int8 | int16 | int32 | int64
}

type unsignedIntegers interface {
	uint8 | uint16 | uint32 | uint64
}

type numeric interface {
	floats | signedIntegers | unsignedIntegers
}

func toTyped[T numeric](v any) (T, bool) {
	switch x := v.(type) {
	case int8:
		return T(x), true
	case int16:
		return T(x), true
	case int32:
		return T(x), true
	case int64:
		return T(x), true
	case uint8:
		return T(x), true
	case uint16:
		return T(x), true
	case uint32:
		return T(x), true
	case uint64:
		return T(x), true
	case float32:
		return T(x), true
	case float64:
		return T(x), true
	}

	return 0, false
}
