package br.ufmg.dcc.lemonade.udfs;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;

public class CompleteCpfCnpjUDF implements UDF2<String, String, String> {

    private int DIVISOR = 11;

    private String calculate_first_digit (String number) {

        int[] CPF_WEIGHTS = {10, 9, 8, 7, 6, 5, 4, 3, 2};
        int[] CNPJ_WEIGHTS = {5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};

        
        int sum = 0;
        int[] weights = new int[15];

        if (number.length() == 9)
            weights = CPF_WEIGHTS;
        else
            weights = CNPJ_WEIGHTS;

        for (int i = 0; i  < number.length(); i++) {
            char digit = number.charAt(i);
            sum = (sum + Character.getNumericValue(digit) * weights[i]);
        }

        int rest_division = sum % DIVISOR;

        if (rest_division < 2){
            return "0";
        }

        return "" + (11 - rest_division);

    }

    private String calculate_second_digit(String number) {

        int[] CPF_WEIGHTS = {11, 10, 9, 8, 7, 6, 5, 4, 3, 2};
        int[] CNPJ_WEIGHTS = {6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2};


        int sum = 0;
        int[] weights = new int[15];

        if (number.length() == 10)
            weights = CPF_WEIGHTS;
        else
            weights = CNPJ_WEIGHTS;

        for (int i = 0; i  < number.length(); i++) {
            char digit = number.charAt(i);
            sum = (sum + Character.getNumericValue(digit) * weights[i]);
        }

        int rest_division = sum % DIVISOR;

        if (rest_division < 2){
            return "0";
        }

        return "" + (11 - rest_division);

    }

    @Override
    public String call(String number, String tipo_pessoa) throws Exception {

      if (number == null || number.equals("NULL")){
          return null;
      }

      String number_final = number;
      String clean_number = number.replaceAll("[^0-9]", "");

      if (tipo_pessoa.equals("F")) {
            if (clean_number.length() == 9){
                String first_part = clean_number.substring(0, 9);
                String first_digit = calculate_first_digit(first_part);
                String second_part = first_part + first_digit;
                String second_digit = calculate_second_digit(second_part);
                number_final = second_part + second_digit;
            } else if (clean_number.length() < 9) {
                number_final = number + ("" + (11 - number.length()));
            }
      } else if (tipo_pessoa.equals("J")) {
            if (clean_number.length() == 12){
                String first_part = clean_number.substring(0, 12);
                String first_digit = calculate_first_digit(first_part);
                String second_part = first_part + first_digit;
                String second_digit = calculate_second_digit(second_part);
                number_final = second_part + second_digit;
            } else if (clean_number.length() < 12) {
                number_final = number + ("" + (14 - number.length()));
            }
      }
      return number_final;
  }

}
