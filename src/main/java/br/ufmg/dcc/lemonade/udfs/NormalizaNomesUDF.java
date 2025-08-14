package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;

// Remove acentos e remove caracteres fora do range 32-127 
// Alguns caracteres dentro do range são removidos
public class NormalizaNomesUDF implements UDF1<String, String> {

    @Override
    public String call(String s) throws Exception {
        if (s == null) {
            return null;
        }

        // replaceAll("\\p{M}", "") remove todos os acentos
        String normalized = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");

        String cleaned = normalized.replaceAll("[^\\x20-\\x7F]", "");

        // Caracteres removidos que estão dentro do range:
        // { } ~ ^ ] [ > = < ; |
        cleaned = cleaned.replaceAll("[\\{\\}\\~\\^\\]\\[><=;|]", "");

        return cleaned.trim().toUpperCase();
    }
}