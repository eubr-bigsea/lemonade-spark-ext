package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
// Padronização de formatos de data, introdução de uma string em um formato de data e retorno no padrão dd/MM/yyyy
public class DatePatterningUDF implements UDF1<String,String> {

    @Override
    public String call(String data){
        String [] patterns = {"dd/MM/yy","ddMMyyyy"};

        for (String pattern:patterns){
            try{
                LocalDate date = LocalDate.parse(data, DateTimeFormatter.ofPattern(pattern));
                return date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            }catch (Exception ignored){
            }
        }

        return null;
    }
}

