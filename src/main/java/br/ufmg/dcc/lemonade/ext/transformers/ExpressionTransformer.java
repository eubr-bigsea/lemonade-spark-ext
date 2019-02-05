package br.ufmg.dcc.lemonade.ext.transformers;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 */
public class ExpressionTransformer extends Transformer implements
        MLWritable, MLReadable<ExpressionTransformer>{


    private static final long serialVersionUID = -379340750179642707L;
    private static final String uidStr = Identifiable$.MODULE$.randomUID(
            "ExpressionTransformer");

    private Param<String> outputCol;

    public ExpressionTransformer(){

    }

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return null;
    }

    @Override
    public StructType transformSchema(StructType schema) {
        return null;
    }

    @Override
    public Transformer copy(ParamMap extra) {
        return null;
    }

    @Override
    public String uid() {
        return null;
    }

    @Override
    public MLReader<ExpressionTransformer> read() {
        return null;
    }

    @Override
    public ExpressionTransformer load(String path) {
        //return super.load(path);
        return null;
    }

    @Override
    public MLWriter write() {
        return null;
    }

    @Override
    public void save(String path) throws IOException {
        //super.save(path);
    }

    public Param<String> getOutputCol(){
        return outputCol;
    }

    public void setOutputCol(Param<String> value){
        outputCol = value;
    }

}
