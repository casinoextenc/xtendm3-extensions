import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.temporal.ChronoUnit

/**
 * ConstraintUtil
 * This utily is used to handle constraints
 *
 * Date         Changed By    Description
 * 20230215     ARENARD       Creation
 */

/**
 * Retrieve constraints
 * @returns true if found otherwise false
 */
public class ConstraintUtil extends ExtendM3Utility {
  public boolean isConstraintFound(DatabaseAPI database, LoggerAPI logger, Integer currentCompany, String inORNO, Integer inPONR, Integer inPOSX, String inCUNO, String inCSCD, Integer inHAZI, String inHIE5, String inCFI4, String inPOPN, String inCSNO, String inORCO, Integer inZALC, Integer inZSAN, String inZCA1, String inZCA2, String inZCA3, String inZCA4, String inZCA5, String inZCA6, String inZCA7, String inZCA8, String inZNAG, String inZALI){
    inCUNO = inCUNO==null ? "":inCUNO
    inCSCD = inCSCD==null ? "":inCSCD
    inHIE5 = inHIE5==null ? "":inHIE5
    logger.debug("inCUNO = " + inCUNO)
    logger.debug("inCSCD = " + inCSCD)
    logger.debug("inHIE5 = " + inHIE5)
    Integer constraintCounter = 0
    ExpressionFactory expression_EXT030 = database.getExpressionFactory("EXT030")
    expression_EXT030 = (expression_EXT030.eq("EXCUNO", inCUNO)).or(expression_EXT030.eq("EXCUNO", ""))
    if(inCUNO == ""){
      expression_EXT030 = (expression_EXT030.eq("EXCSCD", inCSCD)).or(expression_EXT030.eq("EXCSCD", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSCD", inCSCD)).or(expression_EXT030.eq("EXCSCD", "")))
    }
    if(inCUNO == "" && inCSCD == ""){
      expression_EXT030 = (expression_EXT030.eq("EXHAZI", inHAZI as String)).or(expression_EXT030.eq("EXHAZI", "0"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHAZI", inHAZI as String)).or(expression_EXT030.eq("EXHAZI", "0")))
    }
    if(inCUNO == "" && inCSCD == "" && inHAZI == 0){
      expression_EXT030 = (expression_EXT030.eq("EXHIE5", inHIE5)).or(expression_EXT030.eq("EXHIE5", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE5", inHIE5)).or(expression_EXT030.eq("EXHIE5", "")))
    }
    // Retrieve EXT030
    Closure<?> outData_EXT030 = { DBContainer EXT030 ->
      logger.debug("EXT030.ZCID = " + EXT030.get("EXZCID"))
      constraintCounter++
    }
    DBAction EXT030_query = database.table("EXT030").index("00").matching(expression_EXT030).selection("EXZCID").build()
    DBContainer EXT030 = EXT030_query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    if(!EXT030_query.readAll(EXT030, 1, outData_EXT030)){
    }
    //if(constraintCounter > 0) {
    //  return true
    //}
    return false
  }
}