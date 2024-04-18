/**
 * name: LstConstraints
 * program: EXT030MI
 * description: Lst contraints for item customer
 *
 * Date         Changed By    Description
 * 20231010     FLEBARS       Creation
 *
 */
public class LstConstraints extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  int currentCompany
  private def datasITEM
  private def datasORDER
  private boolean in60 = false
  private String msgd


  public LstConstraints(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String itno = (String) (mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")

    // todo load default faci
    getItemDatas(itno, "E10")
    if (in60) {
      mi.error(msgd)
      return
    }
    getOrderDatas(cuno)
    if (in60) {
      mi.error(msgd)
      return
    }
    getLineEXT030()
    if (in60) {
      mi.error(msgd)
      return
    }
  }

  /**
   *
   * @param cuno
   */
  public void getOrderDatas(String cuno) {
    //Chain OCUSMA
    String okcscd = ""
    String opcscd = ""
    String okadid = ""

    datasORDER = [
      "CUNO"  : cuno
      , "CSCD": ""
    ]


    DBAction OCUSMA_query = database
      .table("OCUSMA")
      .index("00")
      .selection("OKCUNO"
        , "OKCSCD"
        , "OKADID"
      )
      .build();

    DBContainer OCUSMA_request = OCUSMA_query.getContainer()
    OCUSMA_request.set("OKCONO", currentCompany)
    OCUSMA_request.set("OKCUNO", cuno)

    //if record exists
    if (OCUSMA_query.read(OCUSMA_request)) {
      okadid = OCUSMA_request.getString("OKADID")
      okcscd = OCUSMA_request.getString("OKCSCD")
    }
    if (okadid != "") {
      DBAction OCUSAD_query = database.table("OCUSAD")
        .index("00")
        .selection("OPCSCD")
        .build()
      DBContainer OCUSAD_request = OCUSAD_query.getContainer()
      OCUSAD_request.set("OPCONO", currentCompany)
      OCUSAD_request.set("OPCUNO", cuno)
      OCUSAD_request.set("OPADRT", 1)
      OCUSAD_request.set("OPADID", okadid)
      if (OCUSAD_query.read(OCUSAD_request)) {
        opcscd = OCUSAD_request.getString("OPCSCD")
      } else {
        in60 = true
        msgd = "Client ${cuno} inexistant"
        return
      }
    }
    datasORDER["CSCD"] = opcscd != "" ? opcscd : okcscd
  }


  /**
   * Get Item Informations and load them into itemDatas map object
   */
  public void getItemDatas(itno, faci) {
    //GetItem informations
    logger.debug("getItemDatas itno=${itno}")
    datasITEM = [
      "HAZI"  : ""
      , "HIE5": ""
      , "CFI4": ""
      , "SUNO": ""
      , "PROD": ""
      , "SIG6": ""
      , "GRWE": ""
      , "NEWE": ""
      , "ZTGR": ""
      , "ZTNW": ""
      , "CSNO": ""
      , "ORCO": ""
      , "ZALI": ""
      , "ZALC": ""
      , "ZSAN": ""
      , "ZCAP": ""
      , "ZCA1": ""
      , "ZCA2": ""
      , "ZCA3": ""
      , "ZCA4": ""
      , "ZCA5": ""
      , "ZCA6": ""
      , "ZCA7": ""
      , "ZCA8": ""
      , "ZORI": ""
      , "ZPHY": ""
      , "ZAGR": ""
      , "ZNAG": ""
      , "ZQCO": ""
    ]

    double alqt = 0

    String hazi = ""
    String hie5 = ""
    String cfi4 = ""
    String suno = ""
    String prod = ""
    String sig6 = ""
    double grwe = 0
    double newe = 0
    double ztgr = 0
    double ztnw = 0
    String csno = ""
    String orco = ""
    int zali = 0
    int zalc = 0
    int zsan = 0
    String zcap = ""
    String zca1 = ""
    String zca2 = ""
    String zca3 = ""
    String zca4 = ""
    String zca5 = ""
    String zca6 = ""
    String zca7 = ""
    String zca8 = ""
    int zori = 0
    int zphy = 0
    int zagr = 0
    String znag = ""

    //Get infos from MITMAS
    DBAction MITMAS_query = database.table("MITMAS").index("00").selection("MMHAZI", "MMHIE5", "MMCFI4", "MMSUNO", "MMPROD", "MMITGR", "MMGRWE", "MMNEWE").build()
    DBContainer MITMAS = MITMAS_query.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itno)
    if (MITMAS_query.read(MITMAS)) {
      hazi = MITMAS.get("MMHAZI")
      hie5 = MITMAS.get("MMHIE5")
      cfi4 = MITMAS.get("MMCFI4")
      suno = MITMAS.get("MMSUNO")
      prod = MITMAS.get("MMPROD")
      grwe = MITMAS.get("MMGRWE") as Double
      newe = MITMAS.get("MMNEWE") as Double
      if (grwe != 0 && alqt != 0) {
        ztgr = grwe * alqt
      }
      if (newe != 0 && alqt != 0) {
        ztnw = newe * alqt
      }
    } else {
      in60 = true
      msgd = "Article ${itno} inexistant"
      return
    }
    
    //Get infos from MITPOP
    ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
    expression_MITPOP = expression_MITPOP.eq("MPREMK", "SIGMA6")
    DBAction MITPOP_query = database.table("MITPOP")
      .index("00")
      .matching(expression_MITPOP)
      .selection("MPPOPN").build()

    DBContainer MITPOP_request = MITPOP_query.getContainer()
    MITPOP_request.set("MPCONO", currentCompany)
    MITPOP_request.set("MPALWT", 1)
    MITPOP_request.set("MPALWQ", "")
    MITPOP_request.set("MPITNO", itno)

    Closure<?> MITPOP_reader = { DBContainer MITPOP_result ->
      //logger.debug("found MITPOP")
      sig6 = MITPOP_result.getString("MPPOPN").trim()
    }

    if (!MITPOP_query.readAll(MITPOP_request, 4, MITPOP_reader)) {
    }

    //Get infos from MITFAC
    DBAction MITFAC_query = database.table("MITFAC")
      .index("00")
      .selection("M9CSNO"
        , "M9ORCO")
      .build()
    DBContainer MITFAC_request = MITFAC_query.getContainer()
    MITFAC_request.set("M9CONO", currentCompany)
    MITFAC_request.set("M9FACI", faci)
    MITFAC_request.set("M9ITNO", itno)
    if (MITFAC_query.read(MITFAC_request)) {
      csno = MITFAC_request.get("M9CSNO")
      orco = MITFAC_request.getString("M9ORCO").trim()
    }

    //Get infos from EXT032
    DBAction EXT032_query = database.table("EXT032").index("00").selection("EXZALC", "EXZSAN", "EXZCA1", "EXZCA2", "EXZCA3", "EXZCA4", "EXZCA5", "EXZCA6", "EXZCA7", "EXZCA8", "EXZORI", "EXZPHY", "EXZALI").build()
    DBContainer EXT032_request = EXT032_query.getContainer()
    EXT032_request.set("EXCONO", currentCompany)
    EXT032_request.set("EXPOPN", sig6)
    EXT032_request.set("EXSUNO", suno)
    EXT032_request.set("EXORCO", orco)
    if (EXT032_query.read(EXT032_request)) {
      zalc = EXT032_request.get("EXZALC") as Integer
      zsan = EXT032_request.get("EXZSAN") as Integer
      zcap = EXT032_request.get("EXZCA1")
      zca1 = EXT032_request.get("EXZCA1")
      zca2 = EXT032_request.get("EXZCA2")
      zca3 = EXT032_request.get("EXZCA3")
      zca4 = EXT032_request.get("EXZCA4")
      zca5 = EXT032_request.get("EXZCA5")
      zca6 = EXT032_request.get("EXZCA6")
      zca7 = EXT032_request.get("EXZCA7")
      zca8 = EXT032_request.get("EXZCA8")
      zori = EXT032_request.get("EXZORI") as Integer
      zphy = EXT032_request.get("EXZPHY") as Integer
      zagr = EXT032_request.get("EXZAGR") as Integer
      zali = EXT032_request.get("EXZALI") as Integer
    }

    DBAction CUGEX1_CIDMAS_query = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer CUGEX1_CIDMAS = CUGEX1_CIDMAS_query.getContainer()
    CUGEX1_CIDMAS.set("F1CONO", currentCompany)
    CUGEX1_CIDMAS.set("F1FILE", "CIDMAS")
    if (prod.trim() != "") {
      CUGEX1_CIDMAS.set("F1PK01", prod)
    } else {
      CUGEX1_CIDMAS.set("F1PK01", suno)
    }
    CUGEX1_CIDMAS.set("F1PK02", "")
    CUGEX1_CIDMAS.set("F1PK03", "")
    CUGEX1_CIDMAS.set("F1PK04", "")
    CUGEX1_CIDMAS.set("F1PK05", "")
    CUGEX1_CIDMAS.set("F1PK06", "")
    CUGEX1_CIDMAS.set("F1PK07", "")
    CUGEX1_CIDMAS.set("F1PK08", "")
    if (CUGEX1_CIDMAS_query.read(CUGEX1_CIDMAS)) {
      znag = CUGEX1_CIDMAS.get("F1A030")
    }
    double cofa = 0
    double zqco = 0
    DBAction query_MITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = query_MITAUN.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", itno)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "COL")
    if (query_MITAUN.read(MITAUN)) {
      cofa = MITAUN.get("MUCOFA") as Double
      if (alqt != 0 && cofa != 0)
        zqco = alqt / cofa
    }

    //Store datas
    datasITEM["HAZI"] = hazi
    datasITEM["HIE5"] = hie5
    datasITEM["CFI4"] = cfi4
    datasITEM["SUNO"] = suno
    datasITEM["PROD"] = prod
    datasITEM["SIG6"] = sig6
    datasITEM["GRWE"] = grwe as String
    datasITEM["NEWE"] = newe as String
    datasITEM["ZTGR"] = ztgr as String
    datasITEM["ZTNW"] = ztnw as String
    datasITEM["ZQCO"] = zqco as String
    datasITEM["CSNO"] = csno
    datasITEM["ORCO"] = orco
    datasITEM["ZALI"] = zali as String
    datasITEM["ZALC"] = zalc as String
    datasITEM["ZSAN"] = zsan as String
    datasITEM["ZCAP"] = zcap
    datasITEM["ZCA1"] = zca1
    datasITEM["ZCA2"] = zca2
    datasITEM["ZCA3"] = zca3
    datasITEM["ZCA4"] = zca4
    datasITEM["ZCA5"] = zca5
    datasITEM["ZCA6"] = zca6
    datasITEM["ZCA7"] = zca7
    datasITEM["ZCA8"] = zca8
    datasITEM["ZORI"] = zori as String
    datasITEM["ZPHY"] = zphy as String
    datasITEM["ZAGR"] = zagr as String
    datasITEM["ZNAG"] = znag
  }
  /**
   * Query on EXT030 (Constraint matrix)
   */
  public void getLineEXT030() {
    String cuno = datasORDER["CUNO"]
    String cscd = datasORDER["CSCD"]

    String hazi = datasITEM["HAZI"]
    String hie5 = datasITEM["HIE5"]
    String cfi4 = datasITEM["CFI4"]
    String suno = datasITEM["SUNO"]
    String prod = datasITEM["PROD"]
    String sig6 = datasITEM["SIG6"]
    String grwe = datasITEM["GRWE"]
    String newe = datasITEM["NEWE"]
    String ztgr = datasITEM["ZTGR"]
    String ztnw = datasITEM["ZTNW"]
    String csno = datasITEM["CSNO"]
    String orco = datasITEM["ORCO"]
    String zali = datasITEM["ZALI"]
    String zalc = datasITEM["ZALC"]
    String zsan = datasITEM["ZSAN"]
    String zcap = datasITEM["ZCAP"]
    String zca1 = datasITEM["ZCA1"]
    String zca2 = datasITEM["ZCA2"]
    String zca3 = datasITEM["ZCA3"]
    String zca4 = datasITEM["ZCA4"]
    String zca5 = datasITEM["ZCA5"]
    String zca6 = datasITEM["ZCA6"]
    String zca7 = datasITEM["ZCA7"]
    String zca8 = datasITEM["ZCA8"]
    String zori = datasITEM["ZORI"]
    String zphy = datasITEM["ZPHY"]
    String zagr = datasITEM["ZAGR"]
    String znag = datasITEM["ZNAG"]


    ExpressionFactory expression_EXT030 = database.getExpressionFactory("EXT030")
    expression_EXT030 = (expression_EXT030.eq("EXCUNO", cuno)).or(expression_EXT030.eq("EXCUNO", ""))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSCD", cscd)).or(expression_EXT030.eq("EXCSCD", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHAZI", hazi as String)).or(expression_EXT030.eq("EXHAZI", "2")))

    if (hie5 != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(expression_EXT030.eq("EXHIE0", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCFI4", cfi4)).or(expression_EXT030.eq("EXCFI4", "")))


    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXPOPN", sig6)).or(expression_EXT030.eq("EXPOPN", "")))

    if (csno != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0, 16) + "*")).or(expression_EXT030.eq("EXCSNO", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXORCO", orco)).or(expression_EXT030.eq("EXORCO", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALC", zalc as String)).or(expression_EXT030.eq("EXZALC", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZSAN", zsan as String)).or(expression_EXT030.eq("EXZSAN", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAP", zca1)).or(expression_EXT030.eq("EXZCAP", "")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAS", zca1)).or(expression_EXT030.eq("EXZCAS", zca2)).or(expression_EXT030.eq("EXZCAS", zca3)).or(expression_EXT030.eq("EXZCAS", zca4)).or(expression_EXT030.eq("EXZCAS", zca5)).or(expression_EXT030.eq("EXZCAS", zca6)).or(expression_EXT030.eq("EXZCAS", zca7)).or(expression_EXT030.eq("EXZCAS", zca8)).or(expression_EXT030.eq("EXZCAS", "")))

    if (znag != "") {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", znag.substring(0, 4) + "*")).or(expression_EXT030.eq("EXZNAG", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", "")))
    }

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALI", zali as String)).or(expression_EXT030.eq("EXZALI", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZORI", zori as String)).or(expression_EXT030.eq("EXZORI", "2")))

    expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZPHY", zphy as String)).or(expression_EXT030.eq("EXZPHY", "2")))

    if (orco == "FR"){
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZOHF", "0")).or(expression_EXT030.eq("EXZOHF", "2")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZOHF", "1")).or(expression_EXT030.eq("EXZOHF", "2")))
    }


    DBAction EXT030_query = database.table("EXT030")
      .index("20")
      .matching(expression_EXT030)
      .selection("EXZCID"
        , "EXZCOD"
        , "EXSTAT"
        , "EXZBLO"
        , "EXCSCD"
        , "EXCUNO"
        , "EXZCAP"
        , "EXZCAS"
        , "EXORCO"
        , "EXPOPN"
        , "EXHIE0"
        , "EXHAZI"
        , "EXCSNO"
        , "EXZALC"
        , "EXCFI4"
        , "EXZSAN"
        , "EXZNAG"
        , "EXZALI"
        , "EXZPHY"
        , "EXZORI"
        , "EXZOHF"
      ).build()
    DBContainer EXT030 = EXT030_query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXSTAT", "20")

    if (!EXT030_query.readAll(EXT030, 2, EXT030_reader)) {
      in60 = true
      msgd = "Aucun enregistrement pour client:${cuno} sigma:${sig6}"
    }
  }
  /**
   * Read EXT030 constraint
   */
  Closure<?> EXT030_reader = { DBContainer EXT030_result ->
    mi.outData.put("ZCID", EXT030_result.get("EXZCID") as String)
    mi.outData.put("ZCOD", EXT030_result.get("EXZCOD") as String)
    mi.outData.put("STAT", EXT030_result.get("EXSTAT") as String)
    mi.outData.put("ZBLO", EXT030_result.get("EXZBLO") as String)
    mi.outData.put("CSCD", EXT030_result.get("EXCSCD") as String)
    mi.outData.put("CUNO", EXT030_result.get("EXCUNO") as String)
    mi.outData.put("ZCAP", EXT030_result.get("EXZCAP") as String)
    mi.outData.put("ZCAS", EXT030_result.get("EXZCAS") as String)
    mi.outData.put("ORCO", EXT030_result.get("EXORCO") as String)
    mi.outData.put("POPN", EXT030_result.get("EXPOPN") as String)
    mi.outData.put("HIE0", EXT030_result.get("EXHIE0") as String)
    mi.outData.put("HAZI", EXT030_result.get("EXHAZI") as String)
    mi.outData.put("CSNO", EXT030_result.get("EXCSNO") as String)
    mi.outData.put("ZALC", EXT030_result.get("EXZALC") as String)
    mi.outData.put("CFI4", EXT030_result.get("EXCFI4") as String)
    mi.outData.put("ZSAN", EXT030_result.get("EXZSAN") as String)
    mi.outData.put("ZNAG", EXT030_result.get("EXZNAG") as String)
    mi.outData.put("ZALI", EXT030_result.get("EXZALI") as String)
    mi.outData.put("ZPHY", EXT030_result.get("EXZPHY") as String)
    mi.outData.put("ZORI", EXT030_result.get("EXZORI") as String)
    mi.outData.put("ZOHF", EXT030_result.get("EXZOHF") as String)
    mi.write()
  }

}
