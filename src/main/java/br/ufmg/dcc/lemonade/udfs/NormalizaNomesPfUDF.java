package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;

// Remove acentos e remove todos characteres que não são letras entre 'a' e 'z' , hifen '-' ou espaços ' '
public class NormalizaNomesPfUDF implements UDF1<String, String> {

    @Override
    public String call(String s) throws Exception {
        if (s == null) {
            return null;
        }

        // replaceAll("\\p{M}", "") remove todos os acentos
        String normalized = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
   
        // Remove todos characteres que não são letras entre 'a' e 'z' , hifen '-' ou espaços ' '
        String cleaned = normalized.replaceAll("[^\\p{L} -]", "");

        return cleaned.toUpperCase();
    }
}