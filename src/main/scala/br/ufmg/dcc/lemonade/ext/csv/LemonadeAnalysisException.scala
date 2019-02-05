package br.ufmg.dcc.lemonade.ext.csv

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  */
class LemonadeAnalysisException(
                                 override val message: String,
                                 override val line: Option[Int] = None,
                                 override val startPosition: Option[Int] = None,
                                 // Some plans fail to serialize due to bugs in scala collections.
                                 @transient override val plan: Option[LogicalPlan] = None,
                                 override val cause: Option[Throwable] =
                                 None) extends AnalysisException(message,
  line, startPosition, plan, cause) {

}
