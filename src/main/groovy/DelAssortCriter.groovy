/**
 * This extension is used by Mashup
 * Name : EXT020MI.DelAssortCriter
 * COMX01 Gestion des assortiments clients
 * Description : The DelAssortCriter transaction delete records to the EXT020 table.
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01 - Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor*/

import java.time.LocalDateTime

public class DelAssortCriter extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public DelAssortCriter(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String cuno = ""
    String ascd = ""
    String fdat = ""
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    }

    if (mi.in.get("ASCD") != null) {
      ascd = mi.in.get("ASCD")
    }

    if (mi.in.get("FDAT") != null) {
      fdat = mi.in.get("FDAT")
    }
    // Delete records from EXT020
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.set("EXASCD", ascd)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    Closure<?> ext020Updater = { LockedResult ext020LockedResult ->
      ext020LockedResult.delete()
    }
    if (!ext020Query.readLock(ext020Request, ext020Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
    // Delete EXT021
    DBAction ext021Query = database.table("EXT021").index("00").build()
    DBContainer ext021Request = ext021Query.getContainer()
    ext021Request.set("EXCONO", currentCompany)
    ext021Request.set("EXCUNO", cuno)
    ext021Request.set("EXASCD", ascd)
    ext021Request.setInt("EXFDAT", fdat as Integer)
    Closure<?> ext021Reader = { DBContainer ext021Result ->
      Closure<?> ext021Updater = { LockedResult ext021LockedResult ->
        ext021LockedResult.delete()
      }
      ext021Query.readLock(ext021Result, ext021Updater)
    }
    if (!ext021Query.readAll(ext021Request, 4, 10000, ext021Reader)) {
    }
  }

}
