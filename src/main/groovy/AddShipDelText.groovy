/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.AddShipDelText
 * Description : Adds shipment delivery text
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 * 20250428     ARENARD      DCONSI.TXID update has been replaced by CRS980MI.SetTextID
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddShipDelText extends ExtendM3Transaction {
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

  public AddShipDelText(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    long txid  = (Long)(mi.in.get("TXID") != null ? mi.in.get("TXID") : 0)
    String kfld = (mi.in.get("KFLD") != null ? (String)mi.in.get("KFLD") : "")
    String file = (mi.in.get("FILE") != null ? (String)mi.in.get("FILE") : "")

    long kfldFile = kfld as long

    if(!"DCONSI00".equalsIgnoreCase(file.trim()) && !"MHDISH00".equalsIgnoreCase(file.trim())) {
      mi.error("paramètre file doit être DCONSI00 ou MHDISH00")
      return
    }


    if(file.trim() == "DCONSI00") {
      DBAction queryDconsi = database.table("DCONSI").index("00").selection("DACONO", "DACONN", "DATXID").build()
      DBContainer DCONSI = queryDconsi.getContainer()
      DCONSI.set("DACONO", currentCompany)
      DCONSI.set("DACONN", kfldFile)
      if(queryDconsi.read(DCONSI)){
        executeCRS980MISetTextID("DCONSI00", txid as String, currentCompany as String, DCONSI.get("DACONN") as String)
      }
    }

    if(file.trim() == "MHDISH00") {
      DBAction queryExt410 = database.table("EXT410").index("00").selection("EXCONO", "EXDLIX").build()
      DBContainer EXT410 = queryExt410.getContainer()
      EXT410.set("EXCONO", currentCompany)
      EXT410.set("EXDLIX",  kfldFile)
      if(queryExt410.read(EXT410)){
        Closure<?> updateEXT410 = { LockedResult lockedResultEXT410 ->
          lockedResultEXT410.set("EXTXID", txid)
          lockedResultEXT410.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT410.setInt("EXCHNO", ((Integer)lockedResultEXT410.get("EXCHNO") + 1))
          lockedResultEXT410.set("EXCHID", program.getUser())
          lockedResultEXT410.update()
        }
        queryExt410.readLock(EXT410, updateEXT410)
      } else {
        EXT410.set("EXCONO", currentCompany)
        EXT410.set("EXDLIX", kfldFile)
        EXT410.set("EXTXID", txid)
        EXT410.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        EXT410.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        EXT410.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        EXT410.set("EXCHNO", 1)
        EXT410.set("EXCHID", program.getUser())
        queryExt410.insert(EXT410)
      }
    }
  }
  /**
   * This method is used to set the text ID in the CRS980MI file.
   * @param FILE The file name.
   * @param TXID The transaction ID.
   * @param KV01 The first key value.
   * @param KV02 The second key value.
   */
  private executeCRS980MISetTextID(String FILE, String TXID, String KV01, String KV02) {
    Map<String, String> parameters = ["FILE": FILE, "TXID": TXID, "KV01": KV01, "KV02": KV02]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("CRS980MI", "SetTextID", parameters, handler)
  }
}
