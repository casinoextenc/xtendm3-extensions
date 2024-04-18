/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT030MI.UpdConstraint
 * Description : Upd records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String NBNR
  private String ZGKY
  private Integer zblc
  private Integer ztps

  public UpdConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    int zcid = (mi.in.get("ZCID") != null ? (Integer)mi.in.get("ZCID") : 0)
    String zcod = (mi.in.get("ZCOD") != null ? (String)mi.in.get("ZCOD") : "")
    String stat = (mi.in.get("STAT") != null ? (String)mi.in.get("STAT") : "")
    int zblo = (mi.in.get("ZBLO") != null ? (Integer)mi.in.get("ZBLO") : 0)
    String cscd = (mi.in.get("CSCD") != null ? (String)mi.in.get("CSCD") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    String zcap = (mi.in.get("ZCAP") != null ? (String)mi.in.get("ZCAP") : "")
    String zcas = (mi.in.get("ZCAS") != null ? (String)mi.in.get("ZCAS") : "")
    String orco = (mi.in.get("ORCO") != null ? (String)mi.in.get("ORCO") : "")
    String popn = (mi.in.get("POPN") != null ? (String)mi.in.get("POPN") : "")
    String hie0 = (mi.in.get("HIE0") != null ? (String)mi.in.get("HIE0") : "")
    int hazi = (mi.in.get("HAZI") != null ? (Integer)mi.in.get("HAZI") : 2)
    String csno = (mi.in.get("CSNO") != null ? (String)mi.in.get("CSNO") : "")
    int zalc = (mi.in.get("ZALC") != null ? (Integer)mi.in.get("ZALC") : 2)
    String cfi4 = (mi.in.get("CFI4") != null ? (String)mi.in.get("CFI4") : "")
    int zsan = (mi.in.get("ZSAN") != null ? (Integer)mi.in.get("ZSAN") : 2)
    String znag = (mi.in.get("ZNAG") != null ?(String) mi.in.get("ZNAG") : "")
    int zali = (mi.in.get("ZALI") != null ? (Integer)mi.in.get("ZALI") : 2)
    int zphy = (mi.in.get("ZPHY") != null ? (Integer)mi.in.get("ZPHY") : 2)
    int zori = (mi.in.get("ZORI") != null ? (Integer)mi.in.get("ZORI") : 2)
    int zohf = (mi.in.get("ZOHF") != null ? (Integer)mi.in.get("ZOHF") : 2)

    //Check if record exists in Constraint Code Table (EXT034)
    if (zcod.length() > 0 && zcod.trim() != "?") {
      DBAction queryEXT034 = database.table("EXT034").index("00").build()
      DBContainer EXT034 = queryEXT034.getContainer()
      EXT034.set("EXCONO", currentCompany)
      EXT034.set("EXZCOD", zcod)
      if (!queryEXT034.read(EXT034)) {
        mi.error("Code contrainte " + zcod + " n'existe pas")
        return
      }
    }

    // check Status
    if(stat == ""){
      stat = "10"
    }
    if (stat != "10" && stat != "20" && stat != "90"){
      mi.error("Statut autorisé : 10, 20 ou 90")
      return
    }

    // check assortment
    if (zblo != 0 && zblo != 1){
      mi.error("L'indicateur dangerosité ZBLO doit être égal à 0 ou 1")
    }

    //Check if record exists in country Code Table (EXT034)
    if (cscd.length() > 0) {
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", cscd)
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code pays " + cscd + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (cuno.length() > 0) {
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer ContainerOCUSMA = queryOCUSMA.getContainer()
      ContainerOCUSMA.set("OKCONO", currentCompany)
      ContainerOCUSMA.set("OKCUNO", cuno)
      if (!queryOCUSMA.read(ContainerOCUSMA)) {
        mi.error("Code client " + cuno + " n'existe pas")
        return
      }
    }
    //Check if record in Feature Contraint Table (EXT033)
    if (zcap.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zcap)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte principale (EXT033) " + zcap + " n'existe pas")
        return
      }
    }

    //Check if record in Feature Contraint Table (EXT033)
    if (zcas.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zcas)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte secondaire (EXT033) " + zcas + " n'existe pas")
        return
      }
    }

    //Check if Origine exists in country Code Table (CSYTAB)
    if (orco.length() > 0) {
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", orco)
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code origine " + orco + " n'existe pas")
        return
      }
    }

    //Check if SIGMA6 exists in MITPOP
    if (popn.length() > 0) {
      ExpressionFactory expression = database.getExpressionFactory("MITPOP")
      expression = expression.ge("MPREMK", "SIGMA6")
      DBAction queryMITPOP = database.table("MITPOP").index("10").matching(expression).build()
      DBContainer ContainerMITPOP = queryMITPOP.getContainer()
      ContainerMITPOP.set("MPCONO", currentCompany)
      ContainerMITPOP.setInt("MPALWT", 1)
      ContainerMITPOP.set("MPALWQ", "")
      ContainerMITPOP.set("MPPOPN", popn)
      if (!queryMITPOP.readAll(ContainerMITPOP, 4, MITPOPData)) {
        mi.error("SIGMA6 " + popn + " n'existe pas")
        return
      }
    }
    // check Dangerous
    if (hazi != 0 && hazi != 1 && hazi != 2){
      mi.error("L'indicateur dangerosité HAZI doit être égal à 0, 1 ou 2")
      return
    }

    //Check if Origine exists in country Code Table (CSYCSN)
    if (csno.length() > 0 && !csno.contains("*")) {
      DBAction queryCSYCSN = database.table("CSYCSN").index("00").build()
      DBContainer ContainerCSYCSN = queryCSYCSN.getContainer()
      ContainerCSYCSN.set("CKCONO", currentCompany)
      ContainerCSYCSN.set("CKCSNO", csno)
      if (!queryCSYCSN.read(ContainerCSYCSN)) {
        mi.error("Code douane " + csno + " n'existe pas")
        return
      }
    }
    // check alcohol
    if (zalc != 0 && zalc != 1 && zalc != 2){
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0, 1 ou 2")
      return
    }

    // Check control code
    if (cfi4.length() > 0) {
      DBAction countryQuery = database.table("CSYTAB").index("00").build()
      DBContainer CSYTAB = countryQuery.getContainer()
      CSYTAB.set("CTCONO",currentCompany)
      CSYTAB.set("CTSTCO",  "CFI4")
      CSYTAB.set("CTSTKY", cfi4)
      if (!countryQuery.read(CSYTAB)) {
        mi.error("Code régie " + cfi4 + " n'existe pas")
        return
      }
    }

    // check sanitary
    if (zsan != 0 && zsan != 1 && zsan != 2){
      mi.error("L'indicateur sanitaire ZSAN doit être égal à 0, 1 ou 2")
      return
    }

    //Check if Origine exists in country Code Table (MITHRY)
    if (hie0.length() > 0 && !hie0.contains("*")) {
      DBAction queryMITHRY = database.table("MITHRY").index("00").build()
      DBContainer ContainerMITHRY = queryMITHRY.getContainer()
      ContainerMITHRY.set("HICONO", currentCompany)
      ContainerMITHRY.set("HIHLVL", 5)
      ContainerMITHRY.set("HIHIE0", hie0)
      if (!queryMITHRY.read(ContainerMITHRY)) {
        mi.error("Hierarchie " + hie0 + " n'existe pas")
        return
      }
    }

    // check food
    if (zali != 0 && zali != 1 && zali != 2){
      mi.error("L'indicateur alimentaire ZALI doit être égal à 0, 1 ou 2")
      return
    }

    // check PhytoSanitary
    if (zphy != 0 && zphy != 1 && zphy != 2){
      mi.error("L'indicateur Phytosanitaire ZPHY doit être égal à 0, 1 ou 2")
      return
    }

    // check Origin UE
    if (zori != 0 && zori != 1 && zori != 2){
      mi.error("L'indicateur origine UE ZORI doit être égal à 0, 1 ou 2")
      return
    }
    if (zohf != 0 && zohf != 1 && zohf != 2){
      mi.error("L'indicateur hors France ZOHF doit être égal à 0, 1 ou 2")
      return
    }
 
    LocalDateTime timeOfCreation = LocalDateTime.now()

    //Check if record exists
    DBAction queryEXT030 = database.table("EXT030")
        .index("00")
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
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
        )
        .build()

    DBContainer containerEXT030 = queryEXT030.getContainer()
    containerEXT030.set("EXCONO", currentCompany)
    containerEXT030.set("EXZCID", zcid)

    //Record exists
    if (!queryEXT030.read(containerEXT030)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> updateEXT030 = { LockedResult lockedResultEXT030 ->
      lockedResultEXT030.set("EXSTAT", stat)
      lockedResultEXT030.set("EXZBLO", zblo)
      if (mi.in.get("ZCOD") != null)
        lockedResultEXT030.set("EXZCOD", zcod)
      if (mi.in.get("CSCD") != null)
        lockedResultEXT030.set("EXCSCD", cscd)
      if (mi.in.get("CUNO") != null)
        lockedResultEXT030.set("EXCUNO", cuno)
      if (mi.in.get("ZCAP") != null)
        lockedResultEXT030.set("EXZCAP", zcap)
      if (mi.in.get("ZCAS") != null)
        lockedResultEXT030.set("EXZCAS", zcas)
      if (mi.in.get("ORCO") != null)
        lockedResultEXT030.set("EXORCO", orco)
      if (mi.in.get("POPN") != null)
        lockedResultEXT030.set("EXPOPN", popn)
      if (mi.in.get("HIE0") != null)
        lockedResultEXT030.set("EXHIE0", hie0)
      if (mi.in.get("HAZI") != null)
        lockedResultEXT030.set("EXHAZI", hazi)
      if (mi.in.get("CSNO") != null)
        lockedResultEXT030.set("EXCSNO", csno)
      if (mi.in.get("ZALC") != null)
        lockedResultEXT030.set("EXZALC", zalc)
      if (mi.in.get("CFI4") != null)
        lockedResultEXT030.set("EXCFI4", cfi4)
      if (mi.in.get("ZSAN") != null)
        lockedResultEXT030.set("EXZSAN", zsan)
      if (mi.in.get("ZNAG") != null)
        lockedResultEXT030.set("EXZNAG", znag)
      if (mi.in.get("ZALI") != null)
        lockedResultEXT030.set("EXZALI", zali)
      if (mi.in.get("ZPHY") != null)
        lockedResultEXT030.set("EXZPHY", zphy)
      if (mi.in.get("ZORI") != null)
        lockedResultEXT030.set("EXZORI", zori)


      lockedResultEXT030.set("EXZOHF", zohf)
      lockedResultEXT030.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResultEXT030.set("EXCHNO", ((Integer)lockedResultEXT030.get("EXCHNO") + 1))
      lockedResultEXT030.set("EXCHID", program.getUser())
      lockedResultEXT030.update()
    }

    queryEXT030.readLock(containerEXT030, updateEXT030)
  }

  Closure<?> MITPOPData = { DBContainer ContainerMITPOP ->
    String itno = ContainerMITPOP.get("MPITNO")
  }
}