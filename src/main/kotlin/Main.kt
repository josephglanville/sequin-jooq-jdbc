import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.jooq.impl.DSL.*
import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp

fun main() {
    val tbl = table(name("public", "every_episode_ever"))

    val id = field(name("id"), String::class.java)
    val airdate = field(name("airdate"), Date::class.java)
    val createdTime = field(name("created_time"), Timestamp::class.java)
    val directedBy = field(name("directed_by"), String::class.java)
    val episodeName = field(name("episode_name"), String::class.java)
    val episodeNumber = field(name("episode_number"), String::class.java)
    val myImpressions = field(name("my_impressions"), String::class.java)
    val notes = field(name("notes"), String::class.java)
    val relatedBooks = field(name("related_books"), String::class.java)
    val runtime = field(name("runtime"), BigDecimal::class.java)
    val series = field(name("series"), String::class.java)
    val stardate = field(name("stardate"), String::class.java)
    val synopsis = field(name("synopsis"), String::class.java)
    val tags = field(name("tags"), String::class.java)
    val watched = field(name("watched"), Boolean::class.java)
    val writtenBy = field(name("written_by"), String::class.java)
    val syncInsertedAt = field(name("_sync_inserted_at"), Timestamp::class.java)
    val syncUpdatedAt = field(name("_sync_updated_at"), Timestamp::class.java)

    val fields = listOf(
        id,
        airdate,
        createdTime,
        directedBy,
        episodeName,
        episodeNumber,
        myImpressions,
        notes,
        relatedBooks,
        runtime,
        series,
        stardate,
        synopsis,
        tags,
        watched,
        writtenBy,
        syncInsertedAt,
        syncUpdatedAt
    )

    val allegianceId = "rec391WpVRSAsFpeq"

    HikariDataSource(
        HikariConfig().apply {
            maximumPoolSize = 100
            minimumIdle = 1
            jdbcUrl = "jdbc:postgresql://us-west-2.aws.sequindb.io:5432/dbvc555d3zygpjk"
            dataSourceProperties = mapOf(
                "user" to "ru3wsifzjqdb6ny.sync_016a91e9",
                "password" to System.getenv("PGPASSWORD")
            ).toProperties()
        }
    ).connection.use { conn ->
        using(conn).also { ctx ->
            // First a simple select to check everything is working
            ctx.select(fields).from(tbl).limit(1).fetchOne()!!.map { r ->
                println(r.intoMap())
            }
            // Then a simple update to make sure we can write
            val updated = ctx.update(tbl).set(notes, "testing1").where(id.eq(allegianceId)).limit(1).execute()
            assert(updated == 1)

            // Ok now UPDATE ... RETURNING
            ctx.update(tbl).set(notes, "testing2").where(id.eq(allegianceId)).returning(fields).fetchOne()!!.map { r ->
                println(r.intoMap())
            }

            // Ok now in a transaction
            ctx.transactionResult { txn ->
                txn.dsl().update(tbl).set(notes, "testing3").where(id.eq(allegianceId)).returning(fields).fetchOne()
            }.let { r ->
                if (r == null) {
                    error("No result from RETURNING :(")
                } else {
                    println(r.intoMap())
                }
            }
        }
    }
}