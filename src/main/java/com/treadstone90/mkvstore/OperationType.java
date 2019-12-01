package com.treadstone90.mkvstore;

public enum OperationType {
    VALUE_OP,
    DELETE_OP;

    public static OperationType fromInteger(int x) {
        switch(x) {
            case 0:
                return VALUE_OP;
            case 1:
                return DELETE_OP;
        }
        return null;
    }
}
