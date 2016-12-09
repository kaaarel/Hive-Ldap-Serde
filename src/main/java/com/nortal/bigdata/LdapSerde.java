package com.nortal.bigdata;

import org.apache.directory.shared.ldap.entry.BinaryValue;
import org.apache.directory.shared.ldap.entry.Entry;
import org.apache.directory.shared.ldap.entry.StringValue;
import org.apache.directory.shared.ldap.entry.Value;
import org.apache.directory.shared.ldap.entry.client.DefaultClientAttribute;
import org.apache.directory.shared.ldap.ldif.LdapLdifException;
import org.apache.directory.shared.ldap.ldif.LdifEntry;
import org.apache.directory.shared.ldap.ldif.LdifReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.AbstractDeserializer;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


import java.util.*;

/**
 * Created by Marko Kaar.
 */

public class LdapSerde extends AbstractDeserializer {

    ObjectInspector inspector;
    ArrayList<Object> row;
    int numColumns;
    List<String> columnNames;


    @Override
    public void initialize(Configuration cfg, Properties props) throws SerDeException {
        String columnNameProperty = props.getProperty(Constants.LIST_COLUMNS);
        columnNames = Arrays.asList(columnNameProperty.split(","));
        numColumns = columnNames.size();
        String columnTypeProperty = props.getProperty(Constants.LIST_COLUMN_TYPES);
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        // Ensure we have the same number of column names and types
        assert numColumns == columnTypes.size();

        List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(numColumns);
        row = new ArrayList<Object>(numColumns);
        for (int c = 0; c < numColumns; c++) {
            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(c));
            inspectors.add(oi);
            row.add(null);
        }
        StandardListObjectInspector s;
        inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    }
    LdifReader reader = new LdifReader();

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        Text obj = (Text) writable;
        try {
            List<LdifEntry> ldifEntries = reader.parseLdif(obj.toString());
            Entry entry = ldifEntries.get(0).getEntry();
            List<String> values = new ArrayList<String>();
            row.set(0, entry.getDn().toString());
            String attrValue;
            for (int i = 1; i < numColumns; i++) {
                String columnName = columnNames.get(i).replaceAll("_", "-");
                values.clear();
                if (entry.get(columnName) != null) {
                    Iterator<Value<?>> it = entry.get(columnName).getAll();
                    while (it.hasNext()) {
                        attrValue = ((Value) it.next()).getString();
                        values.add(attrValue);
                    }
                }
                row.set(i, new ArrayList<String>(values));
            }
        } catch (LdapLdifException e) {
            e.printStackTrace();
        }
        return row;
    }


    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return inspector;
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }


}
