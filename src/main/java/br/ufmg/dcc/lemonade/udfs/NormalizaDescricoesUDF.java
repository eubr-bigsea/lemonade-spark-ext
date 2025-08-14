package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;

// Remove acentos e remove caracteres fora do range 32-127 
// alguns caracteres fora do range não são removidos
public class NormalizaDescricoesUDF implements UDF1<String, String> {

    @Override
    public String call(String s) throws Exception {
        if (s == null) {
            return null;
        }
        // replaceAll("\\p{M}", "") remove todos os acentos
        String normalized = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        // os codigos não removidos \u00BC\u00BD\u00BE são '¼ ½ ¾'
        String cleaned = normalized.replaceAll(
                // remove tudo que não esteja na lista abaixo
                "[^\\x20-\\x7F£§°ºª²³\u00BC\u00BD\u00BE]", "");

        return cleaned.trim().toUpperCase();
    }
}