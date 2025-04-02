package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SparkSession;
// Retonar um booleano que valida a corretude de códigos CPF ou CNPJ, baseado nos dois últimos dígitos verificadores
public class ValidateCodesUDF implements UDF1<String, Boolean> {
    private static final int[] CPF_WEIGHTS = {10, 9, 8, 7, 6, 5, 4, 3, 2};
    private static final int[] CPF_WEIGHTS2 = {11, 10, 9, 8, 7, 6, 5, 4, 3, 2};
    private static final int[] CNPJ_WEIGHTS = {5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};
    private static final int[] CNPJ_WEIGHTS2 = {6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};

    private String calculateDigit(String number, int[] weights) {
        int sum = 0;
        for (int i = 0; i < number.length(); i++) {
            char digit = number.charAt(i);
            int digitValue = Character.getNumericValue(digit);
            sum += digitValue * weights[i];
        }
        int restDivision = sum % 11;
        return (restDivision < 2) ? "0" : String.valueOf(11 - restDivision);
    }

    @Override
    public Boolean call(String number) throws Exception {
        if (number == null) return false;
        
        String cleanNumber = number.replaceAll("[^0-9]", "");
        if (cleanNumber.length() == 11) {
            if ("00000000000".equals(cleanNumber)){
                return false;
            }
            String firstPart = cleanNumber.substring(0, 9);
            String secondPart = cleanNumber.substring(0, 10);
            String firstDigit = cleanNumber.substring(9, 10);
            String secondDigit = cleanNumber.substring(10, 11);

            return firstDigit.equals(calculateDigit(firstPart, CPF_WEIGHTS)) &&
                    secondDigit.equals(calculateDigit(secondPart, CPF_WEIGHTS2));
        } else if (cleanNumber.length() == 14) {
            String firstPart = cleanNumber.substring(0, 12);
            String secondPart = cleanNumber.substring(0, 13);
            String firstDigit = cleanNumber.substring(12, 13);
            String secondDigit = cleanNumber.substring(13, 14);

            return firstDigit.equals(calculateDigit(firstPart, CNPJ_WEIGHTS)) &&
                    secondDigit.equals(calculateDigit(secondPart, CNPJ_WEIGHTS2));
        }
        return false;
    }
}
