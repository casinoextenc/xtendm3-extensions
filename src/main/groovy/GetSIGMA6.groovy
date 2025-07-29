/**
 * Name : EXT011MI.GetSIGMA6 Version 1.0
 *
 * Description :
 * The API retrieve the SIGMA6 of an item to the input parameters
 * GTIN  (EAN13)
 *

 *
 * Date         Changed By    Version   Description
 * 20250225     PBEAUDOUIN    1.1       Creation GetSIGMA6
 * 20250415     ARENARD       1.2       The code has been checked
 * 20250722     FLEBARS       1.3       Changement des messages d'erreurs
 */


public class GetSIGMA6 extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller
  private int currentCompany

  public GetSIGMA6(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    //INITIALIZE VARIABLES
    currentCompany = 100
    String itno = ""
    String gtin = ""
    String popn = ""

    //Get API GTIN
    if (mi.in.get("GTIN") != null && mi.in.get("GTIN") != "") {
      gtin = (String) mi.in.get("GTIN")
    } else {
      mi.error("EAN Obligatoire")
      return
    }

    ExpressionFactory expressionMITPOP10 = database.getExpressionFactory("MITPOP")
    expressionMITPOP10 = expressionMITPOP10.eq("MPREMK", "EA13")
    DBAction mitpop10Query = database.table("MITPOP")
      .matching(expressionMITPOP10)
      .selection("MPITNO")
      .index("10").build()

    DBContainer mitpop10Request = mitpop10Query.getContainer()
    mitpop10Request.set("MPCONO", currentCompany)
    mitpop10Request.set("MPALWT", 1)
    mitpop10Request.set("MPALWQ", "")
    mitpop10Request.set("MPPOPN", gtin)

    //Boucle EAN13 retrieve ITNO
    Closure<?> readMITPOP10 = { DBContainer resultMITPOP10 ->
      itno = resultMITPOP10.getString("MPITNO").trim()
    }
    //Record exists
    if (!mitpop10Query.readAll(mitpop10Request, 4, 1, readMITPOP10)) {
      mi.error("EAN13 ${gtin} n'existe pas")
      return
    } else {
      popn = getS6(itno)
    }
    mi.outData.put("POPN", popn)
    mi.write()
  }

  /**
   * Get the SIGMA6 of an item
   * @param itno
   * @return
   */
  private String getS6(String itno) {
    logger.debug("flb itno ${itno}")
    String popn = ""
    ExpressionFactory expressionMITPOP00 = database.getExpressionFactory("MITPOP")
    expressionMITPOP00 = expressionMITPOP00.eq("MPREMK", "SIGMA6")
    DBAction mitpop00Query = database.table("MITPOP").matching(expressionMITPOP00).selection("MPPOPN").index("00").build()

    DBContainer mitpop00Request = mitpop00Query.getContainer()
    mitpop00Request.set("MPCONO", currentCompany)
    mitpop00Request.set("MPALWT", 1)
    mitpop00Request.set("MPALWQ", "")
    mitpop00Request.set("MPITNO", itno)

    //bOUCLE ITNO retrieve SIGMA6
    Closure<?> readMITPOP00 = { DBContainer resultMITPOP00 ->
      popn = resultMITPOP00.getString("MPPOPN").trim()
    }
    //Record exists
    if (!mitpop00Query.readAll(mitpop00Request, 4, 1, readMITPOP00)) {
      mi.error("SIGMA6 pour ITNO : " + itno + "inexistant")
      return
    }
    return popn
  }

}
