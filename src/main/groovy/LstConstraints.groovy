/**
 * name: LstConstraints
 * program: EXT030MI
 * description: Lst contraints for item customer
 * QUAX01 Gestion du référentiel qualité
 * Date         Changed By    Description
 * 20231010     FLEBARS       Creation
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class LstConstraints extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  int currentCompany
  private Map<String, String> datasITEM
  private Map<String, String> datasORDER
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

    getItemDatas(itno, "E10")// E10 is the main FACI
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
      .build()

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
   * Get Item Informations and store them into itemDatas map object
   * @param itno
   * @param faci
   */
  public void getItemDatas(itno, faci) {
    //GetItem informations
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
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMHAZI", "MMHIE5", "MMCFI4", "MMSUNO", "MMPROD", "MMITGR", "MMGRWE", "MMNEWE").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", itno)
    if (mitmasQuery.read(mitmasRequest)) {
      hazi = mitmasRequest.get("MMHAZI")
      hie5 = mitmasRequest.get("MMHIE5")
      cfi4 = mitmasRequest.get("MMCFI4")
      suno = mitmasRequest.get("MMSUNO")
      prod = mitmasRequest.get("MMPROD")
      grwe = mitmasRequest.get("MMGRWE") as Double
      newe = mitmasRequest.get("MMNEWE") as Double
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
    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")
    DBAction mitpopQuery = database.table("MITPOP")
      .index("00")
      .matching(mitpopExpression)
      .selection("MPPOPN").build()

    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPALWQ", "")
    mitpopRequest.set("MPITNO", itno)

    Closure<?> mitpopReader = { DBContainer mitpopResult ->
      sig6 = mitpopResult.getString("MPPOPN").trim()
    }

    if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReader)) {
    }

    //Get infos from MITFAC
    DBAction mitfacQuery = database.table("MITFAC")
      .index("00")
      .selection("M9CSNO"
        , "M9ORCO")
      .build()
    DBContainer mitfacRequest = mitfacQuery.getContainer()
    mitfacRequest.set("M9CONO", currentCompany)
    mitfacRequest.set("M9FACI", faci)
    mitfacRequest.set("M9ITNO", itno)
    if (mitfacQuery.read(mitfacRequest)) {
      csno = mitfacRequest.get("M9CSNO")
      orco = mitfacRequest.getString("M9ORCO").trim()
    }

    //Get infos from EXT032
    DBAction ext032Query = database.table("EXT032").index("00").selection("EXZALC", "EXZSAN", "EXZCA1", "EXZCA2", "EXZCA3", "EXZCA4", "EXZCA5", "EXZCA6", "EXZCA7", "EXZCA8", "EXZORI", "EXZPHY", "EXZALI").build()
    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXPOPN", sig6)
    ext032Request.set("EXSUNO", suno)
    ext032Request.set("EXORCO", orco)
    if (ext032Query.read(ext032Request)) {
      zalc = ext032Request.get("EXZALC") as Integer
      zsan = ext032Request.get("EXZSAN") as Integer
      zcap = ext032Request.get("EXZCA1")
      zca1 = ext032Request.get("EXZCA1")
      zca2 = ext032Request.get("EXZCA2")
      zca3 = ext032Request.get("EXZCA3")
      zca4 = ext032Request.get("EXZCA4")
      zca5 = ext032Request.get("EXZCA5")
      zca6 = ext032Request.get("EXZCA6")
      zca7 = ext032Request.get("EXZCA7")
      zca8 = ext032Request.get("EXZCA8")
      zori = ext032Request.get("EXZORI") as Integer
      zphy = ext032Request.get("EXZPHY") as Integer
      zagr = ext032Request.get("EXZAGR") as Integer
      zali = ext032Request.get("EXZALI") as Integer
    }

    DBAction cugex1CidmasQuery = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer cugex1CidmasRequest = cugex1CidmasQuery.getContainer()
    cugex1CidmasRequest.set("F1CONO", currentCompany)
    cugex1CidmasRequest.set("F1FILE", "CIDMAS")
    if (prod.trim() != "") {
      cugex1CidmasRequest.set("F1PK01", prod)
    } else {
      cugex1CidmasRequest.set("F1PK01", suno)
    }
    cugex1CidmasRequest.set("F1PK02", "")
    cugex1CidmasRequest.set("F1PK03", "")
    cugex1CidmasRequest.set("F1PK04", "")
    cugex1CidmasRequest.set("F1PK05", "")
    cugex1CidmasRequest.set("F1PK06", "")
    cugex1CidmasRequest.set("F1PK07", "")
    cugex1CidmasRequest.set("F1PK08", "")
    if (cugex1CidmasQuery.read(cugex1CidmasRequest)) {
      znag = cugex1CidmasRequest.get("F1A030")
    }
    double cofa = 0
    double zqco = 0
    DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itno)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "COL")
    if (mitaunQuery.read(mitaunRequest)) {
      cofa = mitaunRequest.get("MUCOFA") as Double
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

    ExpressionFactory ext030Expression = database.getExpressionFactory("EXT030")
    ext030Expression = (ext030Expression.eq("EXCUNO", cuno)).or(ext030Expression.eq("EXCUNO", ""))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSCD", cscd)).or(ext030Expression.eq("EXCSCD", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXHAZI", hazi as String)).or(ext030Expression.eq("EXHAZI", "2")))

    if (hie5 != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(ext030Expression.eq("EXHIE0", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXCFI4", cfi4)).or(ext030Expression.eq("EXCFI4", "")))


    ext030Expression = ext030Expression.and((ext030Expression.eq("EXPOPN", sig6)).or(ext030Expression.eq("EXPOPN", "")))

    if (csno != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 16) + "*")).or(ext030Expression.eq("EXCSNO", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXORCO", orco)).or(ext030Expression.eq("EXORCO", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALC", zalc as String)).or(ext030Expression.eq("EXZALC", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZSAN", zsan as String)).or(ext030Expression.eq("EXZSAN", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAP", zca1)).or(ext030Expression.eq("EXZCAP", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAS", zca1)).or(ext030Expression.eq("EXZCAS", zca2)).or(ext030Expression.eq("EXZCAS", zca3)).or(ext030Expression.eq("EXZCAS", zca4)).or(ext030Expression.eq("EXZCAS", zca5)).or(ext030Expression.eq("EXZCAS", zca6)).or(ext030Expression.eq("EXZCAS", zca7)).or(ext030Expression.eq("EXZCAS", zca8)).or(ext030Expression.eq("EXZCAS", "")))

    if (znag != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", znag.substring(0, 4) + "*")).or(ext030Expression.eq("EXZNAG", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALI", zali as String)).or(ext030Expression.eq("EXZALI", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZORI", zori as String)).or(ext030Expression.eq("EXZORI", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZPHY", zphy as String)).or(ext030Expression.eq("EXZPHY", "2")))

    if (orco == "FR"){
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZOHF", "0")).or(ext030Expression.eq("EXZOHF", "2")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZOHF", "1")).or(ext030Expression.eq("EXZOHF", "2")))
    }

    DBAction ext030Query = database.table("EXT030")
      .index("20")
      .matching(ext030Expression)
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
    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXSTAT", "20")
    if (!ext030Query.readAll(ext030Request, 2, 10000, ext030Reader)) {
      in60 = true
      msgd = "Aucun enregistrement pour client:${cuno} sigma:${sig6}"
    }
  }
  /**
   * Read EXT030 constraint
   */
  Closure<?> ext030Reader = { DBContainer EXT030_result ->
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
