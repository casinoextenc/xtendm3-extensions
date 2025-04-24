/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.AddConstraint
 * Description : Add records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
 * 20250121     YJANNIN      QUAX01 - Controle code pour validation Infor
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private String nbnr
  private int currentCompany

  public AddConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as int
      String currentUser = program.getUser()
      if (!checkCompany(currentCompany, currentUser)) {
        mi.error("Company ${currentCompany} does not exist for the user ${currentUser}")
        return
      }
    }

    //Get mi inputs
    String zcod = (mi.in.get("ZCOD") != null ? (String) mi.in.get("ZCOD") : "")
    String stat = (mi.in.get("STAT") != null ? (String) mi.in.get("STAT") : "")
    int zblo = (mi.in.get("ZBLO") != null ? (Integer) mi.in.get("ZBLO") : 0)
    String cscd = (mi.in.get("CSCD") != null ? (String) mi.in.get("CSCD") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    String zcap = (mi.in.get("ZCAP") != null ? (String) mi.in.get("ZCAP") : "")
    String zcas = (mi.in.get("ZCAS") != null ? (String) mi.in.get("ZCAS") : "")
    String orco = (mi.in.get("ORCO") != null ? (String) mi.in.get("ORCO") : "")
    String popn = (mi.in.get("POPN") != null ? (String) mi.in.get("POPN") : "")
    String hie0 = (mi.in.get("HIE0") != null ? (String) mi.in.get("HIE0") : "")
    int hazi = (mi.in.get("HAZI") != null ? (Integer) mi.in.get("HAZI") : 2)
    String csno = (mi.in.get("CSNO") != null ? (String) mi.in.get("CSNO") : "")
    int zalc = (mi.in.get("ZALC") != null ? (Integer) mi.in.get("ZALC") : 2)
    String cfi4 = (mi.in.get("CFI4") != null ? (String) mi.in.get("CFI4") : "")
    int zsan = (mi.in.get("ZSAN") != null ? (Integer) mi.in.get("ZSAN") : 2)
    String znag = (mi.in.get("ZNAG") != null ? (String) mi.in.get("ZNAG") : "")
    int zali = (mi.in.get("ZALI") != null ? (Integer) mi.in.get("ZALI") : 2)
    int zphy = (mi.in.get("ZPHY") != null ? (Integer) mi.in.get("ZPHY") : 2)
    int zori = (mi.in.get("ZORI") != null ? (Integer) mi.in.get("ZORI") : 2)
    int zohf = (mi.in.get("ZOHF") != null ? (Integer) mi.in.get("ZOHF") : 2)

    //Check if record exists in Constraint Code Table (EXT034)
    if (zcod.length() > 0) {
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", zcod)
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code contrainte " + zcod + " n'existe pas")
        return
      }
    }

    // check Status
    if (stat == "") {
      stat = "10"
    }
    if (stat != "10" && stat != "20" && stat != "90") {
      mi.error("Statut autorisé : 10, 20 ou 90")
      return
    }

    // check assortment
    if (zblo != 0 && zblo != 1) {
      mi.error("L'indicateur dangerosité ZBLO doit être égal à 0 ou 1")
      return
    }

    //Check if record exists in country Code Table (EXT034)
    if (cscd.length() > 0) {
      DBAction csytabCscdQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabCscdRequest = csytabCscdQuery.getContainer()
      csytabCscdRequest.set("CTCONO", currentCompany)
      csytabCscdRequest.set("CTSTCO", "CSCD")
      csytabCscdRequest.set("CTSTKY", cscd)
      if (!csytabCscdQuery.read(csytabCscdRequest)) {
        mi.error("Code pays " + cscd + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (cuno.length() > 0) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cuno)
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code client " + cuno + " n'existe pas")
        return
      }
    }
    //Check if record in Feature Contraint Table (EXT033)
    if (zcap.length() > 0) {
      DBAction ext033query = database.table("EXT033").index("00").build()
      DBContainer ext033request = ext033query.getContainer()
      ext033request.set("EXCONO", currentCompany)
      ext033request.set("EXZCAR", zcap)
      if (!ext033query.read(ext033request)) {
        mi.error("Caractéristique de contrainte principale (EXT033) " + zcap + " n'existe pas")
        return
      }
    }

    //Check if record in Feature Contraint Table (EXT033)
    if (zcas.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zcas)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte secondaire (EXT033) " + zcas + " n'existe pas")
        return
      }
    }

    //Check if Origine exists in country Code Table (CSYTAB)
    if (orco.length() > 0) {
      DBAction csytabCscdQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabCscdRequest = csytabCscdQuery.getContainer()
      csytabCscdRequest.set("CTCONO", currentCompany)
      csytabCscdRequest.set("CTSTCO", "CSCD")
      csytabCscdRequest.set("CTSTKY", orco)
      if (!csytabCscdQuery.read(csytabCscdRequest)) {
        mi.error("Code origine " + orco + " n'existe pas")
        return
      }
    }

    //Check if SIGMA6 exists in MITPOP
    if (popn.length() > 0) {
      ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
      mitpopExpression = mitpopExpression.ge("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("10").matching(mitpopExpression).build()
      DBContainer mitpopRequest = mitpopQuery.getContainer()
      mitpopRequest.set("MPCONO", currentCompany)
      mitpopRequest.setInt("MPALWT", 1)
      mitpopRequest.set("MPALWQ", "")
      mitpopRequest.set("MPPOPN", popn)
      if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReader)) {
        mi.error("SIGMA6 " + popn + " n'existe pas")
        return
      }
    }
    // check Dangerous
    if (mi.in.get("HAZI") != null) {
      if (hazi != 0 && hazi != 1 && hazi != 2) {
        mi.error("L'indicateur dangerosité HAZI doit être égal à 0 ou 1")
        return
      }
    }

    //Check if Origine exists in country Code Table (CSYCSN)
    if (csno.length() > 0 && !csno.contains("*")) {
      DBAction csycsnQuery = database.table("CSYCSN").index("00").build()
      DBContainer csycsnRequest = csycsnQuery.getContainer()
      csycsnRequest.set("CKCONO", currentCompany)
      csycsnRequest.set("CKCSNO", csno)
      if (!csycsnQuery.read(csycsnRequest)) {
        mi.error("Code douane " + csno + " n'existe pas")
        return
      }
    }
    // check alcohol
    if (mi.in.get("ZALC") != null) {
      if (zalc != 0 && zalc != 1 && zalc != 2) {
        mi.error("L'indicateur d'alcool ZALC doit être égal à 0, 1 ou 2")
        return
      }
    }

    // Check control code
    if (cfi4.length() > 0) {
      DBAction countryQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = countryQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CFI4")
      csytabRequest.set("CTSTKY", cfi4)
      if (!countryQuery.read(csytabRequest)) {
        mi.error("Code régie " + cfi4 + " n'existe pas")
        return
      }
    }

    // check sanitary
    if (mi.in.get("ZSAN") != null) {
      if (zsan != 0 && zsan != 1 && zsan != 2) {
        mi.error("L'indicateur sanitaire ZSAN doit être égal à 0, 1 ou 2")
        return
      }
    }

    //Check if Origine exists in country Code Table (MITHRY)
    if (hie0.length() > 0 && !hie0.contains("*")) {
      DBAction mithryQuery = database.table("MITHRY").index("00").build()
      DBContainer mithryRequest = mithryQuery.getContainer()
      mithryRequest.set("HICONO", currentCompany)
      mithryRequest.set("HIHLVL", 5)
      mithryRequest.set("HIHIE0", hie0)
      if (!mithryQuery.read(mithryRequest)) {
        mi.error("Hierarchie " + hie0 + " n'existe pas")
        return
      }
    }

    // check food
    if (mi.in.get("ZALI") != null) {
      if (zali != 0 && zali != 1 && zali != 2) {
        mi.error("L'indicateur alimentaire ZALI doit être égal à 0, 1 ou 2")
        return
      }
    }

    // check PhytoSanitary
    if (mi.in.get("ZPHY") != null) {
      if (zphy != 0 && zphy != 1 && zphy != 2) {
        mi.error("L'indicateur Phytosanitaire ZPHY doit être égal à 0, 1 ou 2")
        return
      }
    }

    // check Origin UE
    if (mi.in.get("ZORI") != null) {
      if (zori != 0 && zori != 1 && zori != 2) {
        mi.error("L'indicateur origine UE ZORI doit être égal à 0, 1 ou 2")
        return
      }
    }
    if (zohf != 0 && zohf != 1 && zohf != 2) {
      mi.error("L'indicateur hors France ZOHF doit être égal à 0, 1 ou 2")
      return
    }
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext030Query = database.table("EXT030").index("00").build()
    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    // Retrieve constraint ID
    executeCRS165MIRtvNextNumber("ZA", "A")
    ext030Request.set("EXZCID", nbnr as Integer)
    if (!ext030Query.read(ext030Request)) {
      ext030Request.set("EXZCOD", zcod)
      ext030Request.set("EXSTAT", stat)
      ext030Request.set("EXCSCD", cscd)
      ext030Request.set("EXCUNO", cuno)
      ext030Request.set("EXZCAP", zcap)
      ext030Request.set("EXZCAS", zcas)
      ext030Request.set("EXORCO", orco)
      ext030Request.set("EXPOPN", popn)
      ext030Request.set("EXHIE0", hie0)
      ext030Request.set("EXHAZI", hazi)
      ext030Request.set("EXCSNO", csno)
      ext030Request.set("EXZALC", zalc)
      ext030Request.set("EXCFI4", cfi4)
      ext030Request.set("EXZNAG", znag)
      ext030Request.set("EXZSAN", zsan)
      ext030Request.set("EXZALI", zali)
      ext030Request.set("EXZPHY", zphy)
      ext030Request.set("EXZORI", zori)
      ext030Request.set("EXZOHF", zohf)
      ext030Request.set("EXZBLO", zblo)
      ext030Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext030Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext030Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext030Request.setInt("EXCHNO", 1)
      ext030Request.set("EXCHID", program.getUser())
      ext030Query.insert(ext030Request)
      String constraintID = ext030Request.get("EXZCID")
      mi.outData.put("ZCID", constraintID)
      mi.write()
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String nbty, String nbid) {
    Map<String, String> parameters = ["NBTY": nbty, "NBID": nbid]
    Closure<?> handler = { Map<String, String> response ->
      nbnr = response.NBNR.trim()
      if (response.error != null) {
        return mi.error("Failed CRS165MI.RtvNextNumber: " + response.errorMessage)
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }

  Closure<?> mitpopReader = { DBContainer mitpopResult ->
    String itno = mitpopResult.get("MPITNO")
  }

  /**
   *  Check if CONO is alowed for user
   * @param cono
   * @param user
   * @return true if alowed false otherwise
   */
  private boolean checkCompany(int cono, String user) {
    DBAction csyusrQuery = database.table("CSYUSR").index("00").build()
    DBContainer csyusrRequest = csyusrQuery.getContainer()
    csyusrRequest.set("CRCONO", cono)
    csyusrRequest.set("CRDIVI", '')
    csyusrRequest.set("CRRESP", user)
    if (!csyusrQuery.read(csyusrRequest)) {
      return false
    }
    return true
  }

}
