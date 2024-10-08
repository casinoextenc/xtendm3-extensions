
/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.GetShipDelText
 * Description : Get shipment delivery text
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class GetShipDelText extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm

  private String jobNumber

  public GetShipDelText(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    LocalDateTime timeOfCreation = LocalDateTime.now()

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    String kfld = (mi.in.get("KFLD") != null ? (String)mi.in.get("KFLD") : "")
    String file = (mi.in.get("FILE") != null ? (String)mi.in.get("FILE") : "")

    long kfld_file = kfld as long

    if(!"DCONSI00".equalsIgnoreCase(file.trim()) && !"MHDISH00".equalsIgnoreCase(file.trim())) {
      mi.error("paramètre file doit être DCONSI00 ou MHDISH00")
    }


    if(file.trim() == "DCONSI00") {
      DBAction query_DCONSI = database.table("DCONSI").index("00").selection("DATXID").build()
      DBContainer DCONSI = query_DCONSI.getContainer()
      DCONSI.set("DACONO", currentCompany)
      DCONSI.set("DACONN", kfld_file)
      if(query_DCONSI.read(DCONSI)){
        long txid = DCONSI.getLong("DATXID")
        mi.outData.put("TXID", txid.toString())
        mi.write()
      }
    }

    if(file.trim() == "MHDISH00") {
      DBAction query_EXT410 = database.table("EXT410").index("00").selection("EXTXID").build()
      DBContainer EXT410 = query_EXT410.getContainer()
      EXT410.set("EXCONO", currentCompany)
      EXT410.set("EXDLIX",  kfld_file)
      if(query_EXT410.read(EXT410)){
        long txid = EXT410.getLong("EXTXID")
        mi.outData.put("TXID", txid.toString())
        mi.write()
      }
    }
  }
}