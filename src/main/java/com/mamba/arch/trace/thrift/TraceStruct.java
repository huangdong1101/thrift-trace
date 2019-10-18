package com.mamba.arch.trace.thrift;

import lombok.Getter;
import lombok.Setter;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.scheme.StandardScheme;
import org.apache.thrift.scheme.TupleScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TraceStruct<T extends TBase<T, TFieldIdEnum>> implements TBase<TraceStruct<T>, TFieldIdEnum>, Serializable, Cloneable, Comparable<TraceStruct<T>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TraceStruct.class);

    private static final FieldMetaData TRACE_FIELD_META_DATA = new FieldMetaData("trace", TFieldRequirementType.DEFAULT, new MapMetaData(TType.MAP, new FieldValueMetaData(TType.STRING), new FieldValueMetaData(TType.STRING)));

    private static final TFieldIdEnum TRACE_FIELD_ID = new TFieldIdEnum() {
        @Override
        public short getThriftFieldId() {
            return Short.MAX_VALUE;
        }

        @Override
        public String getFieldName() {
            return TRACE_FIELD_META_DATA.fieldName;
        }
    };

    @Getter
    private final T data;

    //unmodifiableMap
    @Getter
    @Setter
    private Map<String, String> trace;

    private transient final TStruct structDesc;

    private transient final Map<TFieldIdEnum, FieldMetaData> metaDataMap;

    private transient final Method validateMethod;

    private transient final StandardScheme<TraceStruct<T>> standardScheme = new ProxyStandardScheme();

    private transient final TupleScheme<TraceStruct<T>> tupleScheme = new ProxyTupleScheme();

    public TraceStruct(T data) {
        this.data = Objects.requireNonNull(data);

        Class<?> dataClass = this.data.getClass();
        try {
            //Field: STRUCT_DESC
            Field structDescField = dataClass.getDeclaredField("STRUCT_DESC");
            structDescField.setAccessible(true);
            this.structDesc = (TStruct) structDescField.get(dataClass);

            //Field: metaDataMap
            Field metaDataMapField = dataClass.getDeclaredField("metaDataMap");
            this.metaDataMap = (Map<TFieldIdEnum, FieldMetaData>) metaDataMapField.get(dataClass);

            //Method: validate
            this.validateMethod = dataClass.getMethod("validate");
        } catch (NoSuchMethodException | NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public TFieldIdEnum fieldForId(int fieldId) {
        if (fieldId == TRACE_FIELD_ID.getThriftFieldId()) {
            return TRACE_FIELD_ID;
        } else {
            return this.data.fieldForId(fieldId);
        }
    }

    /**
     * Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
     */
    @Override
    public boolean isSet(TFieldIdEnum field) {
        if (field == TRACE_FIELD_ID) {
            return trace != null;
        } else {
            return data.isSet(field);
        }
    }

    @Override
    public Object getFieldValue(TFieldIdEnum field) {
        if (field == TRACE_FIELD_ID) {
            return this.trace;
        } else {
            return data.getFieldValue(field);
        }
    }

    @Override
    public void setFieldValue(TFieldIdEnum field, Object value) {
        if (field == TRACE_FIELD_ID) {
            this.trace = (Map<String, String>) value;
        } else {
            this.data.setFieldValue(field, value);
        }
    }

    @Override
    public TraceStruct<T> deepCopy() {
        TraceStruct<T> instance = new TraceStruct(this.data.deepCopy());
        if (this.trace != null) {
            instance.trace = Collections.unmodifiableMap(new LinkedHashMap<>(this.trace));
        }
        return instance;
    }

    @Override
    public void clear() {
        this.data.clear();
        this.trace = null;
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }
        if (!(that instanceof TraceStruct)) {
            return false;
        }
        return this.equals((TraceStruct) that);
    }

    public boolean equals(TraceStruct that) {
        if (that == null) {
            return false;
        }
        if (this == that) {
            return true;
        }
        if (!this.data.equals(that.data)) {
            return false;
        }
        return Objects.equals(this.trace, that.trace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.data, this.trace);
    }

    @Override
    public int compareTo(TraceStruct<T> other) {
        int comparison = this.data.compareTo(other.data);
        if (comparison != 0) {
            return comparison;
        }
        //TODO: trace
        return 0;
    }

    @Override
    public void read(TProtocol iprot) throws TException {
        if (StandardScheme.class.equals(iprot.getScheme())) {
            standardScheme.read(iprot, this);
        } else {
            tupleScheme.read(iprot, this);
        }
    }

    @Override
    public void write(TProtocol oprot) throws TException {
        if (StandardScheme.class.equals(oprot.getScheme())) {
            standardScheme.write(oprot, this);
        } else {
            tupleScheme.write(oprot, this);
        }
    }

    public void validate() throws TException {
        if (this.validateMethod == null) {
            return;
        }
        try {
            this.validateMethod.invoke(this.data);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();
            if (target instanceof TException) {
                throw (TException) target;
            } else {
                throw new RuntimeException(target);
            }
        }
    }

    private static Object readFieldValue(TProtocol iprot, FieldValueMetaData valueMetaData) throws TException {
        switch (valueMetaData.type) {
            case TType.BYTE:
                return iprot.readByte();
            case TType.DOUBLE:
                return iprot.readDouble();
            case TType.I16:
                return iprot.readI16();
            case TType.I32:
                return iprot.readI32();
            case TType.I64:
                return iprot.readI64();
            case TType.STRING:
                if (valueMetaData.isBinary()) {
                    return iprot.readBinary();
                } else {
                    return iprot.readString();
                }
            case TType.STRUCT:
                Class<? extends TBase> structClass = ((StructMetaData) valueMetaData).structClass;
                TBase base;
                try {
                    base = structClass.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
                base.read(iprot);
                return base;
            case TType.MAP:
                FieldValueMetaData mapKeyMetaData = ((MapMetaData) valueMetaData).keyMetaData;
                FieldValueMetaData mapValueMetaData = ((MapMetaData) valueMetaData).valueMetaData;
                TMap tMap = iprot.readMapBegin();
                Map<Object, Object> map = new java.util.LinkedHashMap<>(tMap.size);
                for (int i = 0; i < tMap.size; i++) {
                    Object key = readFieldValue(iprot, mapKeyMetaData);
                    Object value = readFieldValue(iprot, mapValueMetaData);
                    map.put(key, value);
                }
                iprot.readMapEnd();
                return map;
            case TType.SET:
                FieldValueMetaData setElemMetaData = ((SetMetaData) valueMetaData).elemMetaData;
                TSet tSet = iprot.readSetBegin();
                Set<Object> set = new java.util.LinkedHashSet<>(tSet.size);
                for (int i = 0; i < tSet.size; i++) {
                    Object element = readFieldValue(iprot, setElemMetaData);
                    set.add(element);
                }
                iprot.readSetEnd();
                return set;
            case TType.LIST:
                FieldValueMetaData listElemMetaData = ((ListMetaData) valueMetaData).elemMetaData;
                TList tList = iprot.readListBegin();
                List<Object> list = new java.util.ArrayList<>(tList.size);
                for (int i = 0; i < tList.size; i++) {
                    Object element = readFieldValue(iprot, listElemMetaData);
                    list.add(element);
                }
                iprot.readListEnd();
                return list;
            case TType.ENUM:
                int enumValue = iprot.readI32();
                Class<? extends TEnum> enumClass = ((EnumMetaData) valueMetaData).enumClass;
                try {
                    Method method = enumClass.getMethod("findByValue", int.class);
                    Object tEnum = method.invoke(enumClass, enumValue);
                    return tEnum;
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalStateException(e);
                }
            default:
                TProtocolUtil.skip(iprot, valueMetaData.type);
                return null;
        }
    }

    private static void writeFieldValue(TProtocol oprot, FieldValueMetaData valueMetaData, Object value) throws TException {
        switch (valueMetaData.type) {
            case TType.BOOL:
                oprot.writeBool((Boolean) value);
                break;
            case TType.BYTE:
                oprot.writeByte((Byte) value);
                break;
            case TType.DOUBLE:
                oprot.writeDouble((Double) value);
                break;
            case TType.I16:
                oprot.writeI16((Short) value);
                break;
            case TType.I32:
                oprot.writeI32((Integer) value);
                break;
            case TType.I64:
                oprot.writeI64((Long) value);
                break;
            case TType.STRING:
                if (valueMetaData.isBinary()) {
                    oprot.writeBinary((ByteBuffer) value);
                } else {
                    oprot.writeString((String) value);
                }
                break;
            case TType.STRUCT:
                TBase tBase = ((TBase) value);
                tBase.write(oprot);
                break;
            case TType.MAP:
                FieldValueMetaData mapKeyMetaData = ((MapMetaData) valueMetaData).keyMetaData;
                FieldValueMetaData mapValueMetaData = ((MapMetaData) valueMetaData).valueMetaData;
                Map<?, ?> map = (Map<?, ?>) value;
                oprot.writeMapBegin(new TMap(mapKeyMetaData.type, mapValueMetaData.type, map.size()));
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    writeFieldValue(oprot, mapKeyMetaData, entry.getKey());
                    writeFieldValue(oprot, mapValueMetaData, entry.getValue());
                }
                oprot.writeMapEnd();
                break;
            case TType.SET:
                FieldValueMetaData setElemMetaData = ((SetMetaData) valueMetaData).elemMetaData;
                Set<?> set = (Set<?>) value;
                oprot.writeSetBegin(new TSet(setElemMetaData.type, set.size()));
                for (Object element : set) {
                    writeFieldValue(oprot, setElemMetaData, element);
                }
                oprot.writeSetEnd();
                break;
            case TType.LIST:
                FieldValueMetaData listElemMetaData = ((ListMetaData) valueMetaData).elemMetaData;
                List<?> list = (List<?>) value;
                oprot.writeListBegin(new TList(listElemMetaData.type, list.size()));
                for (Object element : list) {
                    writeFieldValue(oprot, listElemMetaData, element);
                }
                oprot.writeListEnd();
                break;
            case TType.ENUM:
                TEnum tEnum = (TEnum) value;
                oprot.writeI32(tEnum.getValue());
                break;
            default:
                break;
        }
    }

    private static class ProxyStandardScheme<T extends TBase<T, TFieldIdEnum>> extends StandardScheme<TraceStruct<T>> {

        @Override
        public void read(TProtocol iprot, TraceStruct<T> struct) throws TException {
            iprot.readStructBegin();
            while (true) {
                TField schemeField = iprot.readFieldBegin();

                if (schemeField.type == TType.STOP) {
                    break;
                }

                TFieldIdEnum fieldId = struct.fieldForId(schemeField.id);
                if (fieldId == null) {
                    TProtocolUtil.skip(iprot, schemeField.type);
                } else if (fieldId == TRACE_FIELD_ID) {
                    Object value = readFieldValue(iprot, TRACE_FIELD_META_DATA.valueMetaData);
                    struct.setFieldValue(TRACE_FIELD_ID, value);
                } else {
                    FieldMetaData fieldMetaData = struct.metaDataMap.get(fieldId);
                    Object value = readFieldValue(iprot, fieldMetaData.valueMetaData);
                    struct.data.setFieldValue(fieldId, value);
                }

                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be checked in the validate method
            struct.validate();
        }

        public void write(TProtocol oprot, TraceStruct<T> struct) throws TException {
            struct.validate();
            oprot.writeStructBegin(struct.structDesc);

            for (Map.Entry<TFieldIdEnum, FieldMetaData> entry : struct.metaDataMap.entrySet()) {
                TFieldIdEnum fieldId = entry.getKey();
                if (!struct.data.isSet(fieldId)) {
                    continue;
                }
                Object value = struct.data.getFieldValue(fieldId);
                if (value == null) {
                    continue;
                }
                FieldValueMetaData valueMetaData = entry.getValue().valueMetaData;

                oprot.writeFieldBegin(new TField(fieldId.getFieldName(), valueMetaData.type, fieldId.getThriftFieldId()));
                writeFieldValue(oprot, valueMetaData, value);
                oprot.writeFieldEnd();
            }

            if (struct.trace != null) {
                oprot.writeFieldBegin(new TField(TRACE_FIELD_ID.getFieldName(), TRACE_FIELD_META_DATA.valueMetaData.type, TRACE_FIELD_ID.getThriftFieldId()));
                writeFieldValue(oprot, TRACE_FIELD_META_DATA.valueMetaData, struct.trace);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }
    }

    private static class ProxyTupleScheme<T extends TBase<T, TFieldIdEnum>> extends TupleScheme<TraceStruct<T>> {

        @Override
        public void read(TProtocol iprot, TraceStruct<T> struct) throws TException {
            this.read((TTupleProtocol) iprot, struct);
        }

        private void read(TTupleProtocol iprot, TraceStruct<T> struct) throws TException {
            BitSet incoming = iprot.readBitSet(struct.metaDataMap.size() + 1);
            int i = 0;
            for (Map.Entry<TFieldIdEnum, FieldMetaData> entry : struct.metaDataMap.entrySet()) {
                if (incoming.get(i++)) {
                    TFieldIdEnum fieldId = entry.getKey();
                    FieldValueMetaData valueMetaData = entry.getValue().valueMetaData;
                    Object value = readFieldValue(iprot, valueMetaData);
                    struct.setFieldValue(fieldId, value);
                }
            }

            if (incoming.get(i)) {
                Object value = readFieldValue(iprot, TRACE_FIELD_META_DATA.valueMetaData);
                struct.setFieldValue(TRACE_FIELD_ID, value);
            }
        }

        @Override
        public void write(TProtocol oprot, TraceStruct<T> struct) throws TException {
            this.write((TTupleProtocol) oprot, struct);
        }

        private void write(TTupleProtocol oprot, TraceStruct<T> struct) throws TException {
            BitSet optionals = new BitSet();
            int bitIndex = 0;
            for (TFieldIdEnum fieldId : struct.metaDataMap.keySet()) {
                if (struct.isSet(fieldId)) {
                    optionals.set(bitIndex);
                }
                bitIndex++;
            }
            if (struct.isSet(TRACE_FIELD_ID)) {
                optionals.set(bitIndex);
            }

            for (Map.Entry<TFieldIdEnum, FieldMetaData> entry : struct.metaDataMap.entrySet()) {
                TFieldIdEnum fieldId = entry.getKey();
                if (!struct.isSet(fieldId)) {
                    continue;
                }
                Object value = struct.getFieldValue(fieldId);
                if (value == null) {
                    continue;
                }
                FieldValueMetaData valueMetaData = entry.getValue().valueMetaData;
                writeFieldValue(oprot, valueMetaData, value);
            }
            if (struct.isSet(TRACE_FIELD_ID)) {
                writeFieldValue(oprot, TRACE_FIELD_META_DATA.valueMetaData, struct.trace);
            }
        }
    }
}