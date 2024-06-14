package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;
// Remove acentos retirando tudo que está fora do conjunto de caracteres ASCII
public class StripAccentsUDF implements UDF1<String, String> {
    @Override
    public String call(String s) throws Exception {
        return s == null ? null 
			: java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD)
                .replaceAll("[^\\p{ASCII}]", "");
    }
}
