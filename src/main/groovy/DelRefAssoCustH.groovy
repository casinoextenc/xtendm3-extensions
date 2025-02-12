/**
 * Name : EXT010MI.DelRefAssoCustH
 * Description :
 * This API method to delete records in specific table EXT010 Customer Assortment
 * COMX01 Gestion des assortiments clients
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */

import java.time.LocalDateTime
import java.time.ZoneOffset

public class DelRefAssoCustH extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public DelRefAssoCustH(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    //Get mi inputs
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")


    //Check if record exists
    DBAction ext010Query = database.table("EXT010")
      .index("02")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT",
        "EXSIG6",
        "EXSAPR",
        "EXSULE",
        "EXSULD",
        "EXFUDS",
        "EXRSCL",
        "EXCMDE",
        "EXFVDT",
        "EXLVDT",
        "EXTVDT",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", cuno)


    //Record exists


    Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
      createEXT011Record(ext010LockedResult, "D")
      ext010LockedResult.delete()
    }

    ext010Query.readAllLock(ext010Request,2, ext010Updater)

  }

  /**
   * Create EXT011 record
   * @parameter DBContainer
   * */
  private void createEXT011Record(DBContainer ext010Request, String flag){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    Long lmts = timeOfCreation.toInstant(ZoneOffset.UTC).toEpochMilli()
    DBAction ext011Query = database.table("EXT011")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT",
        "EXRGDT",
        "EXRGTM",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext011Request = ext011Query.getContainer()
    ext011Request.set("EXCONO", currentCompany)
    ext011Request.set("EXASGD", ext010Request.get("EXASGD"))
    ext011Request.set("EXCUNO", ext010Request.get("EXCUNO"))
    ext011Request.set("EXITNO", ext010Request.get("EXITNO"))
    ext011Request.set("EXSIG6", ext010Request.get("EXSIG6"))
    ext011Request.set("EXSAPR", ext010Request.get("EXSAPR"))
    ext011Request.set("EXSULE", ext010Request.get("EXSULE"))
    ext011Request.set("EXSULD", ext010Request.get("EXSULD"))
    ext011Request.set("EXFUDS", ext010Request.get("EXFUDS"))
    ext011Request.set("EXCDAT", ext010Request.get("EXCDAT"))
    ext011Request.set("EXRSCL", ext010Request.get("EXRSCL"))
    ext011Request.set("EXCMDE", ext010Request.get("EXCMDE"))
    ext011Request.set("EXFVDT", ext010Request.get("EXFVDT"))
    ext011Request.set("EXLVDT", ext010Request.get("EXLVDT"))
    ext011Request.set("EXTVDT", ext010Request.get("EXTVDT"))
    ext011Request.set("EXRGDT", ext010Request.get("EXRGDT"))
    ext011Request.set("EXRGTM", ext010Request.get("EXRGTM"))
    ext011Request.set("EXCHNO", ext010Request.get("EXCHNO"))
    ext011Request.set("EXCHID", ext010Request.get("EXCHID"))
    ext011Request.set("EXLMTS", lmts)
    ext011Request.set("EXFLAG", flag)
    ext011Query.insert(ext011Request)
  }
}

