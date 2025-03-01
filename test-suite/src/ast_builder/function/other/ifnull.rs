use {
    crate::*,
    gluesql_core::{
        ast_builder::{self, *},
        executor::Payload,
        prelude::Value::*,
    },
};

test_case!(ifnull, async move {
    let glue = get_glue!();

    // create table - Foo
    let actual = table("Foo")
        .create_table()
        .add_column("id INTEGER")
        .add_column("name TEXT")
        .add_column("nickname TEXT")
        .execute(glue)
        .await;
    let expected = Ok(Payload::Create);
    test(actual, expected);

    // insert into Foo
    let actual = table("Foo")
        .insert()
        .columns("id, name nickname")
        .values(vec![
            vec![num(100), text("Pickle"), text("Pi")],
            vec![num(200), null(), text("Hello")],
        ])
        .execute(glue)
        .await;
    let expected = Ok(Payload::Insert(2));
    test(actual, expected);

    // Replace with text using ifnull
    let actual = table("Foo")
        .select()
        .project("id")
        .project(col("name").ifnull(text("isnull")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "IFNULL(\"name\", 'isnull')"
        I64 | Str;
        100   "Pickle".to_owned();
        200   "isnull".to_owned()
    ));
    test(actual, expected);

    // Replace with other column using ifnull
    let actual = table("Foo")
        .select()
        .project("id")
        .project(col("name").ifnull(col("nickname")))
        .execute(glue)
        .await;
    let expected = Ok(select!(
        id  | "IFNULL(\"name\", \"nickname\")"
        I64 | Str;
        100   "Pickle".to_owned();
        200   "Hello".to_owned()
    ));
    test(actual, expected);

    // ifnull without table
    let actual = values(vec![
        vec![ast_builder::ifnull(text("HELLO"), text("WORLD"))],
        vec![ast_builder::ifnull(null(), text("WORLD"))],
    ])
    .execute(glue)
    .await;
    let expected = Ok(select!(
        "column1"
        Str;
        "HELLO".to_owned();
        "WORLD".to_owned()
    ));
    test(actual, expected);
});
