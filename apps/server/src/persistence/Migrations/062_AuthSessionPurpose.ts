import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// Legacy sessions stay unclassified; transport and labels cannot backfill purpose.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    ALTER TABLE auth_sessions
    ADD COLUMN purpose TEXT CHECK (purpose IN ('human', 'automation'))
  `;
});
