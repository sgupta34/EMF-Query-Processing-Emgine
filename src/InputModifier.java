import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InputModifier {
    
    // change the initialized 
    public static String SigmaCondition(String str, int sequence) {
        
        // java format for sigma(change selection condition to java conditions)
        String output = str.replaceAll("=", "==").replaceAll(">==", ">=").replaceAll("<==", "<=").replaceAll("<>", "!=").replaceAll(" and | AND ", "&&").replaceAll(" or | OR ", "||").replaceAll(" ", "");
        // check if nothing is mentioned in suchthat clause
        List<String> noNull = new ArrayList<String>();
        // transfer
        if(output.equals("")) {
            return "true";
        }
        String[] substr = output.split("&&|\\|\\|");
        for(String sub : substr) {
            String[] _sub = sub.split("==|!=|>=|<=|>|<");
            // left + operator
            String oper = null;
            if(sub.indexOf("==") != -1) {oper = "==";}
            else if(sub.indexOf("!=") != -1) {oper = "!=";}
            else if(sub.indexOf(">=") != -1) {oper = ">=";}
            else if(sub.indexOf("<=") != -1) {oper = "<=";}
            else if(sub.indexOf(">") != -1) {oper = ">";}
            else {oper = "<";}
            String _ip1 = _sub[0].replaceAll(sequence+".", "") + oper;
            // right
            String _ip2 = _sub[1];
            // first loop for every operands other than avg
            for(Map.Entry<String, String> entry : QueryGenerator.table.entrySet()) {
                String key = entry.getKey();
                // do not mix "quant" with something like "sum_1_quant"
                _ip2 = _ip2.replaceAll("_"+key, "check");
                String[] _key = key.split("_");
                // if key == "avg_N_attr"
                if(_key.length == 3 && _key[0].equals("avg")) {/* do nothing */}
                else {
                    if(_ip2.indexOf(key) != -1) {
                        // add a null value test
                        noNull.add("(!(mfStruct.get(\""+key+"\").get(i)+\"\").equals(\"null\"))&&");
                    }
                    _ip2 = _ip2.replaceAll(key, "("+QueryGenerator.table.get(key)+")mfStruct.get(\""+key+"\").get(i)");
                }
                // recover
                _ip2 = _ip2.replaceAll("check", "_"+key);
            }
            // second loop only for avg
            for(Map.Entry<String, String> entry : QueryGenerator.table.entrySet()) {
                String key = entry.getKey();
                // do not mix "quant" with something like "sum_1_quant"
                _ip2 = _ip2.replaceAll("_"+key, "check");
                String[] _key = key.split("_");
                // if key == "avg_N_attr"
                if(_key.length == 3 && _key[0].equals("avg")) {
                    String _sum = "sum_"+_key[1]+"_"+_key[2];
                    String _count = "count_"+_key[1]+"_"+_key[2];
                    if(_ip2.indexOf(key) != -1) {
                        // add two null value test
                        noNull.add("(!(mfStruct.get(\""+_sum+"\").get(i)+\"\").equals(\"null\"))&&");
                        noNull.add("(!(mfStruct.get(\""+_count+"\").get(i)+\"\").equals(\"null\"))&&");
                    }
                    _ip2 = _ip2.replaceAll(key, "(Double.parseDouble(("+QueryGenerator.table.get(_sum)+")mfStruct.get(\""+_sum+"\").get(i)+\"\")/Double.parseDouble(("+QueryGenerator.table.get(_count)+")mfStruct.get(\""+_count+"\").get(i)+\"\"))");
                }
                else {/* do nothing */}
                // recover
                _ip2 = _ip2.replaceAll("check", "_"+key);
            }
            // 'NY' -> "NY"
            _ip2 = _ip2.replaceAll("'", "\"");
            // put all together
            String subNew = _ip1+_ip2;
            
            String _op = "";
            // a==(!=)b, both sides transfers to String
            if((subNew.indexOf("==") != -1)||(subNew.indexOf("!=") != -1)) {
                String[] subsub = subNew.split("==|!=");
                if(subNew.indexOf("==") != -1) {
                    _op = "("+subsub[0]+".equals("+subsub[1]+"+\"\"))";
                }
                else {
                    _op = "(!"+subsub[0]+".equals("+subsub[1]+"+\"\"))";
                }
            }
            // a>=(<=/>/<)b, both sides transfers to Double, then compare
            else if((subNew.indexOf(">") != -1) || (subNew.indexOf("<") != -1)) {
                String[] subsub = subNew.split(">=|<=|>|<");
                String _oper = null;
                if(subNew.indexOf(">=") != -1) {_oper = ">=";}
                else if(subNew.indexOf("<=") != -1) {_oper = "<=";}
                else if(subNew.indexOf(">") != -1) {_oper = ">";}
                else {_oper = "<";}
                _op = "(Double.parseDouble("+subsub[0]+")"+_oper+"Double.parseDouble("+subsub[1]+"+\"\"))";
            }
            else {
                System.out.println("Unrecognized operation.");
            }
            // replace the original
            output = output.replace(sub, _op);
        }
        //  remove duplicates in noNull
        List<String> noNull2 = new ArrayList<String>();
        String noNullStr = "";
        for (String dup : noNull) {
            if (Collections.frequency(noNull2, dup) < 1) {
                noNull2.add(dup);
                noNullStr += dup;
            }
        }
        
        return noNullStr+output;
    }
    
    // Having clause to Java condition
    public static String HavingCondition(String str) {
        
        // java format
        String output = str.replaceAll("=", "==").replaceAll(">==", ">=").replaceAll("<==", "<=").replaceAll("<>", "!=").replaceAll(" and | AND ", "&&").replaceAll(" or | OR ", "||").replaceAll(" ", "");
        // an extra determine statement (in front of original having clause) that prevent doing operation on null value
        List<String> noNull = new ArrayList<String>();
        // transfer
        if(output.equals("")) {
            return "true";
        }
        String[] substr = output.split("&&|\\|\\|");
        for(String sub : substr) {
            String[] _sub = sub.split("==|!=|>=|<=|>|<");
            // operator
            String oper = null;
            if(sub.indexOf("==") != -1) {oper = "==";}
            else if(sub.indexOf("!=") != -1) {oper = "!=";}
            else if(sub.indexOf(">=") != -1) {oper = ">=";}
            else if(sub.indexOf("<=") != -1) {oper = "<=";}
            else if(sub.indexOf(">") != -1) {oper = ">";}
            else {oper = "<";}
            // left and right
            String _ip1 = _sub[0];
            String _ip2 = _sub[1];
            // first loop for every operands other than avg
            for(Map.Entry<String, String> entry : QueryGenerator.table.entrySet()) {
                String key = entry.getKey();
                // do not mix "quant" with something like "sum_1_quant"
                _ip1 = _ip1.replaceAll("_"+key, "check");
                _ip2 = _ip2.replaceAll("_"+key, "check");
                String[] _key = key.split("_");
                // if key == "avg_N_attr"
                if(_key.length == 3 && _key[0].equals("avg")) {/* do nothing */}
                else {
                    if((_ip1.indexOf(key) != -1)||(_ip2.indexOf(key) != -1)) {
                        // add a null value test
                        noNull.add("(!(mfStruct.get(\""+key+"\").get(i)+\"\").equals(\"null\"))&&");
                    }
                    _ip1 = _ip1.replaceAll(key, "("+QueryGenerator.table.get(key)+")mfStruct.get(\""+key+"\").get(i)");
                    _ip2 = _ip2.replaceAll(key, "("+QueryGenerator.table.get(key)+")mfStruct.get(\""+key+"\").get(i)");
                }
                // recover
                _ip1 = _ip1.replaceAll("check", "_"+key);
                _ip2 = _ip2.replaceAll("check", "_"+key);
            }
            // second loop only for avg
            for(Map.Entry<String, String> entry : QueryGenerator.table.entrySet()) {
                String key = entry.getKey();
                // do not mix "quant" with something like "sum_1_quant"
                _ip1 = _ip1.replaceAll("_"+key, "check");
                _ip2 = _ip2.replaceAll("_"+key, "check");
                String[] _key = key.split("_");
                // if key == "avg_N_attr"
                if(_key.length == 3 && _key[0].equals("avg")) {
                    String _sum = "sum_"+_key[1]+"_"+_key[2];
                    String _count = "count_"+_key[1]+"_"+_key[2];
                    if((_ip1.indexOf(key) != -1)||(_ip2.indexOf(key) != -1)) {
                        // add two null value test
                        noNull.add("(!(mfStruct.get(\""+_sum+"\").get(i)+\"\").equals(\"null\"))&&");
                        noNull.add("(!(mfStruct.get(\""+_count+"\").get(i)+\"\").equals(\"null\"))&&");
                    }
                    _ip1 = _ip1.replaceAll(key, "(Double.parseDouble(("+QueryGenerator.table.get(_sum)+")mfStruct.get(\""+_sum+"\").get(i)+\"\")/Double.parseDouble(("+QueryGenerator.table.get(_count)+")mfStruct.get(\""+_count+"\").get(i)+\"\"))");
                    _ip2 = _ip2.replaceAll(key, "(Double.parseDouble(("+QueryGenerator.table.get(_sum)+")mfStruct.get(\""+_sum+"\").get(i)+\"\")/Double.parseDouble(("+QueryGenerator.table.get(_count)+")mfStruct.get(\""+_count+"\").get(i)+\"\"))");
                }
                else {/* do nothing */}
                // recover
                _ip1 = _ip1.replaceAll("check", "_"+key);
                _ip2 = _ip2.replaceAll("check", "_"+key);
            }
            // 'NY' -> "NY"
            _ip1 = _ip1.replaceAll("'", "\"");
            _ip2 = _ip2.replaceAll("'", "\"");
            // put all together
            String subNew = _ip1+oper+_ip2;
            
            String _op = "";
            // a==(!=)b, both sides transfers to String
            if((subNew.indexOf("==") != -1)||(subNew.indexOf("!=") != -1)) {
                String[] subsub = subNew.split("==|!=");
                if(subNew.indexOf("==") != -1) {
                    _op = "(("+subsub[0]+"+\"\").equals("+subsub[1]+"+\"\"))";
                }
                else {
                    _op = "(!("+subsub[0]+"+\"\").equals("+subsub[1]+"+\"\"))";
                }
            }
            // a>=(<=/>/<)b, both sides transfers to Double, then compare
            else if((subNew.indexOf(">") != -1) || (subNew.indexOf("<") != -1)) {
                String[] subsub = subNew.split(">=|<=|>|<");
                String _oper = null;
                if(subNew.indexOf(">=") != -1) {_oper = ">=";}
                else if(subNew.indexOf("<=") != -1) {_oper = "<=";}
                else if(subNew.indexOf(">") != -1) {_oper = ">";}
                else {_oper = "<";}
                _op = "(Double.parseDouble("+subsub[0]+"+\"\")"+_oper+"Double.parseDouble("+subsub[1]+"+\"\"))";
            }
            else {
                System.out.println("Unrecognized operation.");
            }
            // replace the original
            output = output.replace(sub, _op);
        }
        //  remove duplicates in noNull
        List<String> noNull2 = new ArrayList<String>();
        String noNullStr = "";
        for (String dup : noNull) {
            if (Collections.frequency(noNull2, dup) < 1) {
                noNull2.add(dup);
                noNullStr += dup;
            }
        }
        
        return noNullStr+output;
    }
    
    public static String DataType(String data_type) {
        
        if(data_type.equals("character varying")||data_type.equals("character")) {
            return "String";
        }
        else if(data_type.equals("integer")||data_type.equals("smallint")||data_type.equals("bigint")) {
            return "int";
        }
        else if(data_type.equals("numeric")||data_type.equals("real")||data_type.equals("double precision")) {
            return "Double";
        }
        else {
            System.out.println("Unrecognized data type.");
        }
        return "";
    }
    // specify aggregates' type
    public static String AggregateType(String aggr) {
        
        String type = null;
        String[] spl = aggr.split("_");
        if(spl[0].equals("avg")) {
            type = "Double";
        }
        else if(spl[0].equals("count")) {
            type = "int";
        }
        // max, min, sum
        else {
            type = QueryGenerator.table.get(spl[2]);
        }
        return type;
    }
    
}
