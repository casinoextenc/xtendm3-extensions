/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT033MI.DelConstrFeat
 * Description : Delete records from the EXT033 table.
 * Date         Changed By   Description
 * 20230201     SEAR	        QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class DelConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final ProgramAPI program

  public DelConstrFeat(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    DBAction ext033Query = database.table("EXT033").index("00").build()
    DBContainer ext033Request = ext033Query.getContainer()
    ext033Request.set("EXCONO", currentCompany)
    ext033Request.set("EXZCAR", mi.in.get("ZCAR"))
    Closure<?> ext033Updater = { LockedResult ext033LockedResult ->
      ext033LockedResult.delete()
    }
    if (!ext033Query.readLock(ext033Request, ext033Updater)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
