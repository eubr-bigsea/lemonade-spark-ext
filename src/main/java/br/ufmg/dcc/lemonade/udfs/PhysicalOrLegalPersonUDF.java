package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;
// Retonar 'J' ou 'F' baseado no código da pessoas passado para a função (verifica se é CNPJ ou CPF)
public class PhysicalOrLegalPersonUDF implements UDF1<String,String> {

    @Override
    public String call(String codeCpfCnpj) throws Exception {

        String cleanedString = codeCpfCnpj.replaceAll("[^0-9*]+", "");
        if (cleanedString.length()>11 || cleanedString.startsWith("-")){
            return "J";
        }
        return "F";
    }
}


