/**************************************************************************************** * Name: EXT030MI.AddConstraint
 * Name : EXT038MI.AddPrtContraint
 * Description : Add records to the EXT038 table.
 * Date         Changed By   Description
 * 20250328     MLECLERCQ      QUAX04 - evol print contrainte
 * 20250626     FLEBARS        Code review for validation
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class DltPrtContraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany


  public DltPrtContraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    currentCompany = (Integer)program.getLDAZD().CONO

    //Get mi inputs
    String bjno = (mi.in.get("BJNO") != null ? (String) mi.in.get("BJNO") : "")

    logger.debug("Delete EXT038 records for job number: ${bjno}")


    DBAction ext038Query = database.table("EXT038").index("00").build()
    DBContainer ext038Request = ext038Query.getContainer()
    ext038Request.set("EXCONO", currentCompany)
    ext038Request.set("EXBJNO",bjno as Long)

    Closure<?> ext038Reader = { DBContainer ext038Result ->
      ext038Query.readLock(ext038Result, { LockedResult ext038ResultLockedResult ->
        ext038ResultLockedResult.delete()
      })
    }

    if (!ext038Query.readAll(ext038Request, 2, 10000, ext038Reader)) {
      mi.error("Aucun enregistrement EXT038 trouvé pour le numéro de travail : ${bjno}")
      return
    }
  }
}
