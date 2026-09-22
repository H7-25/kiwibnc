exports.up = async function(knex) {
    await knex.schema.table('users', function (table) {
        table.text('settings');
    });
};

exports.down = function(knex) {
    // Never go backwards in the db
};
