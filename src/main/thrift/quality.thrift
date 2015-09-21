namespace java au.com.cba.omnia.grimlock

struct dateT {
    1: required string   dateVal;
    2: required string   format;
}

struct eventT {
    1: required string   event;
}

union FValue {
    1: bool     booleanVal;
    2: i32      integerVal;
    3: i64      longVal;
    4: double   doubleVal;
    5: string   stringVal;
    6: dateT    dateVal;
    //7: eventT eventVal;
}

struct PositionT3D {
    1: required FValue one;
    2: required FValue two;
    3: required FValue three;
    4: optional FValue featureValue;
}

struct PositionT {
    1: optional string one;
    2: optional string two;
    3: optional string three;
    4: optional string four;
    5: optional string five;
    6: optional FValue featureValue;
}