/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT030MI.SelConstraint
 * Description : Select records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class SelConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private int countLine
  private int nbli
  private int nbsl
  private int maxSel

  public SelConstraint(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    Integer currentCompany
    int zcid = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    String zcod =  (mi.in.get("ZCOD") != null ? (String)mi.in.get("ZCOD") : "")
    String stat = (mi.in.get("STAT") != null ?  (String)mi.in.get("STAT") : "")
    int zblo =  (mi.in.get("ZBLO") != null ? (Integer)mi.in.get("ZBLO") : 0)
    String cscd = (mi.in.get("CSCD") != null ?  (String)mi.in.get("CSCD") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    nbli = (mi.in.get("NBLI") != null ? (Integer)mi.in.get("NBLI") : 0)
    nbsl = (mi.in.get("NBSL") != null ? (Integer)mi.in.get("NBSL") : 50)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Set Record to return
    maxSel = nbli + nbsl

    LocalDateTime timeOfCreation = LocalDateTime.now()
    //Check if record exists
    String constraintID_ex = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    String constraintCode =  (String)(mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
    String countryCode =  (String)(mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
    String status = (String)(mi.in.get("STAT") != null ? mi.in.get("STAT") : "")
    String customer = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String assortmentBloc = (mi.in.get("ZBLO") != null ? (Integer)mi.in.get("ZBLO") : 0)
    ExpressionFactory expression = database.getExpressionFactory("EXT030")

    int countExpression = 0

    if (mi.in.get("ZCID") != null) {
      expression = expression.eq("EXZCID", constraintID_ex)
      countExpression++
    }

    if (constraintCode.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXZCOD", constraintCode)
      } else {
        expression = expression.and(expression.eq("EXZCOD", constraintCode))
      }
      countExpression++
    }

    if (countryCode.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXCSCD", countryCode)
      } else {
        expression = expression.and(expression.eq("EXCSCD", countryCode))
      }
      countExpression++
    }

    if (status.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXSTAT", status)
      } else {
        expression = expression.and(expression.eq("EXSTAT", status))
      }
      countExpression++
    }

    if (customer.length() > 0) {
      if (countExpression == 0) {
        expression = expression.eq("EXCUNO", customer)
      } else {
        expression = expression.and(expression.eq("EXCUNO", customer))
      }
      countExpression++
    }

    if (mi.in.get("ZBLO") != null) {
      if (countExpression == 0) {
        expression = expression.eq("EXZBLO", assortmentBloc)
      } else {
        expression = expression.and(expression.eq("EXZBLO", assortmentBloc))
      }
      countExpression++
    }

    DBAction queryEXT030 = database.table("EXT030")
        .index("00")
        .matching(expression)
        .selection(
        "EXZCID",
        "EXZCOD",
        "EXSTAT",
        "EXZBLO",
        "EXCSCD",
        "EXCUNO",
        "EXZCAP",
        "EXZCAS",
        "EXORCO",
        "EXPOPN",
        "EXHIE0",
        "EXHAZI",
        "EXCSNO",
        "EXZALC",
        "EXCFI4",
        "EXZSAN",
        "EXZNAG",
        "EXZALI",
        "EXZORI",
        "EXZPHY",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXZOHF",
        "EXCHID"
        )
        .build()

    DBContainer containerEXT030 = queryEXT030.getContainer()
    containerEXT030.set("EXCONO", currentCompany)
    
    //Record exists
    if (!queryEXT030.readAll(containerEXT030, 1, outData)){
      //mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> outData = { DBContainer containerEXT030 ->
    countLine++
    logger.debug("countLine=${countLine}")
    logger.debug("nbli=${nbli}")
    logger.debug("maxSel=${maxSel}")
    if (countLine > nbli && countLine <= maxSel) {
      String constraintCode = containerEXT030.get("EXZCOD")
      String constraintID = containerEXT030.get("EXZCID")
      String status = containerEXT030.get("EXSTAT")
      String countryCode = containerEXT030.get("EXCSCD")
      String blocAssortment = containerEXT030.get("EXZBLO")
      String customerCode = containerEXT030.get("EXCUNO")
      String constraintTypeP = containerEXT030.get("EXZCAP")
      String constraintTypeS = containerEXT030.get("EXZCAS")
      String originCountry = containerEXT030.get("EXORCO")
      String Sigma6 = containerEXT030.get("EXPOPN")
      String Hierarchy = containerEXT030.get("EXHIE0")
      String dangerous = containerEXT030.get("EXHAZI")
      String statisticNumber = containerEXT030.get("EXCSNO")
      String alcohol = containerEXT030.get("EXZALC")
      String ruleCode = containerEXT030.get("EXCFI4")
      String sanitary = containerEXT030.get("EXZSAN")
      String agreementNumber = containerEXT030.get("EXZNAG")
      String foodProduct = containerEXT030.get("EXZALI")
      String originUE = containerEXT030.get("EXZORI")
      String phytosanitary = containerEXT030.get("EXZPHY")
      String entryDate = containerEXT030.get("EXRGDT")
      String entryTime = containerEXT030.get("EXRGTM")
      String changeDate = containerEXT030.get("EXLMDT")
      String changeNumber = containerEXT030.get("EXCHNO")
      String changedBy = containerEXT030.get("EXCHID")
      mi.outData.put("ZCOD", constraintCode)
      mi.outData.put("ZCID", constraintID)
      mi.outData.put("STAT", status)
      mi.outData.put("CSCD", countryCode)
      mi.outData.put("ZBLO", blocAssortment)
      mi.outData.put("CUNO", customerCode)
      mi.outData.put("ZCAP", constraintTypeP)
      mi.outData.put("ZCAS", constraintTypeS)
      mi.outData.put("ORCO", originCountry)
      mi.outData.put("POPN", Sigma6)
      mi.outData.put("HIE0", Hierarchy)
      mi.outData.put("HAZI", dangerous)
      mi.outData.put("CSNO", statisticNumber)
      mi.outData.put("ZALC", alcohol)
      mi.outData.put("CFI4", ruleCode)
      mi.outData.put("ZSAN", sanitary)
      mi.outData.put("ZNAG", agreementNumber)
      mi.outData.put("ZALI", foodProduct)
      mi.outData.put("ZORI", originUE)
      mi.outData.put("ZPHY", phytosanitary)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.outData.put("ZOHF", containerEXT030.get("EXZOHF") as String)
      mi.write()
    }
    
    if (countLine > maxSel) {
      return
    }
  }
}