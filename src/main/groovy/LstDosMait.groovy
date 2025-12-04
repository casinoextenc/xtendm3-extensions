/****************************************************************************************
 Extension Name: EXT050MI.LstDosMait
 Type: ExtendM3Transaction
 Script Author: SEAR
 Date: 2023-05-11
 Description:
 * batch template

 Revision History:
 Name                    Date             Version          Description of Changes
 SEAR                    2023-05-11       1.0              LOG28 - Creation of files and containers
 MLECLERCQ               2023-08-11       1.1              LOG28 - bugs correction
 MLECLERCQ               2023-08-18       1.2              LOG28 - REJECT ORTP = C20
 MLECLERCQ               2023-08-18       1.3              LOG28 - ROUT from OOLINE instead of OOHEAD
 MLECLERCQ               2023-08-18       1.4              LOG28 - Corrected FRLD instead of FLRD in Mi Inputs
 MLECLERCQ               2024-04-30       1.5              LOG28 - added country name
 ARENARD                 2025-04-28       1.6              Output fields description added
 MLECLERCQ               2025-05-13       1.7              Added OBORST > 20 filter on OOLINE
 MLECLERCQ               2025-11-14       1.8              Changed read on OOLINE to MITPLO due to volume
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstDosMait extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  int currentCompany

  private String fortInput
  private String tortInput
  private String frldInput
  private String trldInput
  private String whloInput
  private String ornoInput
  private String cunoInput
  private String cunm
  private String cscd
  private String cscn

  private String rout
  private String massification

  private Integer nbMaxRecord = 10000

  private ArrayList<String> allowedOrders

  public LstDosMait(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentCompany = (Integer)program.getLDAZD().CONO

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    ornoInput = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    fortInput = (mi.in.get("FORT") != null ? (String)mi.in.get("FORT") : "")
    tortInput = (mi.in.get("TORT") != null ? (String)mi.in.get("TORT") : "")
    frldInput = (mi.in.get("FRLD") != null ? (String)mi.in.get("FRLD") : "")
    trldInput = (mi.in.get("TRLD") != null ? (String)mi.in.get("TRLD") : "")
    cunoInput = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")

    // check warehouse
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    if(cunoInput){
      // check Customer
      ExpressionFactory ocusmaExp = database.getExpressionFactory("OCUSMA")
      ocusmaExp = ocusmaExp.eq("OKSTAT", "20")
      DBAction queryOcusma = database.table("OCUSMA").index("00").matching(ocusmaExp).selection("OKCUNO").build()
      DBContainer OCUSMA = queryOcusma.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cunoInput)
      if(!queryOcusma.read(OCUSMA)){
        mi.error("Client " + cunoInput + " n'existe pas ou statut non valide")
        return
      }
    }

    if(ornoInput!="") {
      getOolineData(ornoInput)
    }else{
      allowedOrders = new ArrayList<String>()
      // Get OOLINE
      ExpressionFactory mitploExp = database.getExpressionFactory("MITPLO")
      mitploExp = mitploExp.lt("MOSTS2", "44")
      mitploExp = mitploExp.and(mitploExp.gt("MOSTS2", "20"))
      mitploExp = mitploExp.and(mitploExp.eq("MOORCA","311"))
      DBAction mitploQuery = database.table("MITPLO").index("00").matching(mitploExp).selection("MORIDN","MOWHLO","MOTRTP").build()
      DBContainer mitploRequest = mitploQuery.getContainer()
      mitploRequest.set("MOCONO", currentCompany)
      mitploRequest.set("MOWHLO", whloInput)

      Closure<?> mitploReader = { DBContainer mitploResult ->

        String orno = mitploResult.get("MORIDN")
        //rout = oolineResult.get("OBROUT")

        if(allowedOrders.size() > 0){
          boolean  found = allowedOrders.find { it -> it == orno}
          if(!found){
            allowedOrders.add(orno)

            getOolineData(orno)
          }
        }else{
          allowedOrders.add(orno)
          //rout = oolineResult.get("OBROUT")
          getOolineData(orno)
        }

      }

      if (!mitploQuery.readAll(mitploRequest, 2, nbMaxRecord, mitploReader)){
      }
    }
  }

  public void getOolineData(String orno){
    // Get OOLINE
    ExpressionFactory oolineExp = database.getExpressionFactory("OOLINE")
    oolineExp = oolineExp.lt("OBORST", "44")
    oolineExp = oolineExp.and(oolineExp.gt("OBORST", "20"))
    oolineExp = oolineExp.and(oolineExp.eq("OBWHLO", whloInput))
    DBAction oolineQuery = database.table("OOLINE").index("00").matching(oolineExp).selection("OBORNO","OBWHLO","OBROUT").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", orno)

    Closure<?> oolineReader = { DBContainer oolineResult ->

      rout = oolineResult.get("OBROUT")
      getOohead(orno)
    }

    if (!oolineQuery.readAll(oolineRequest, 2, 1, oolineReader)){
    }
  }

  /**
   * Read OOHEAD data
   * @param orno
   * @return
   */
  public void getOohead(String orno) {

    ExpressionFactory ooheadExp = database.getExpressionFactory("OOHEAD")
    ooheadExp = ooheadExp.eq("OAUCA4", "")
    if(fortInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.ge("OAORTP", fortInput))
    }
    if(tortInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.le("OAORTP", tortInput))
    }
    if(frldInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.ge("OARLDT", frldInput))
    }
    if(trldInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.le("OARLDT", trldInput))
    }

    if(cunoInput!="") {
      ooheadExp = ooheadExp.and(ooheadExp.eq("OACUNO", cunoInput))
    }

    ooheadExp = ooheadExp.and(ooheadExp.ne("OAORTP","C20"))
    ooheadExp = ooheadExp.and(ooheadExp.ge("OAORSL","20"))

    DBAction ooheadQuery = database.table("OOHEAD").index("00").matching(ooheadExp).selection(
      "OACONO"
      ,"OAORNO"
      ,"OAORTP"
      ,"OARLDT"
      ,"OACUNO"
      ,"OACUOR"
      ,"OARESP"
      ,"OACUOR"
      ,"OAUCA4"
      ,"OAUCA5"
      ,"OAUCA6"
    ).build()

    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      String ortp = ooheadRequest.get("OAORTP") as String
      String cuno = ooheadRequest.get("OACUNO") as String
      String cuor = ooheadRequest.get("OACUOR") as String
      String rldt = ooheadRequest.get("OARLDT") as String
      String resp = ooheadRequest.get("OARESP") as String
      String uca4 = ooheadRequest.get("OAUCA4") as String
      String uca5 = ooheadRequest.get("OAUCA5") as String
      String uca6 = ooheadRequest.get("OAUCA6") as String

      DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNM","OKCSCD").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO",currentCompany)
      ocusmaRequest.set("OKCUNO",cuno)
      if (ocusmaQuery.read(ocusmaRequest)) {
        cunm = ocusmaRequest.get("OKCUNM") as String
        cscd = ocusmaRequest.get("OKCSCD") as String
        getCountryName(cscd)
        getMassification(cuno, rldt, orno)
      }
      //set output data
      mi.outData.put("ORNO", orno)
      mi.outData.put("ORTP", ortp)
      mi.outData.put("CUNO", cuno)
      mi.outData.put("CUNM", cunm)
      mi.outData.put("CUOR", cuor)
      mi.outData.put("CSCD", cscd)
      mi.outData.put("CSCN",cscn)
      mi.outData.put("RLDT", rldt)
      mi.outData.put("RESP", resp)
      mi.outData.put("UCA4", uca4)
      mi.outData.put("UCA5", uca5)
      mi.outData.put("UCA6", uca6)
      mi.outData.put("ROUT", rout)
      mi.outData.put("MASS", massification)
      mi.write()

    }
  }

  /**
   * Get country name
   * @param countryCode
   * @return
   */
  private getCountryName(countryCode){
    DBAction csytabQuery = database.table("CSYTAB").index("20").selection("CTTX40").build()
    DBContainer csytabRequest = csytabQuery.getContainer()
    csytabRequest.set("CTCONO", currentCompany)
    csytabRequest.set("CTSTCO", 'CSCD')
    csytabRequest.set("CTSTKY", countryCode)

    csytabQuery.readAll(csytabRequest, 3,1, { DBContainer csytabClosure ->
      cscn = csytabClosure.get("CTTX40") as String
    })
  }

  /**
   * Get massification
   * @param cuno
   * @param livDate
   * @param orno
   * @return
   */
  private getMassification(cuno, livDate,orno){
    ExpressionFactory exprExt014 = database.getExpressionFactory("EXT014")

    exprExt014 = exprExt014.le("EXFVDT",livDate.toString())
    exprExt014 = exprExt014.and(exprExt014.ge("EXLVDT",livDate.toString()))

    DBAction ext014Query = database.table("EXT014").index("00").matching(exprExt014).selection("EXCONO","EXCUNO","EXWHLO","EXFVDT","EXLVDT").build()
    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.set("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO",whloInput)


    if (!ext014Query.readAll(ext014Request,3,1,{})) {
      massification = "0"
    }else{
      massification = "1"
    }
  }
}
