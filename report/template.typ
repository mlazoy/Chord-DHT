#let report(
  title: "",
  subtitle: "",
  authors: (),
  team: "",
  semester: "",
  doc,
) = {
  // Set the document metadata.
  set document(title: title, author: authors)

  // Language for the document.
  set text(lang: "el")

  // General styles and heading settings.
  set heading(numbering: none)

  // Title page layout.
  set align(center)
  text(14pt, "ΣΧΟΛΗ ΗΛΕΚΤΡΟΛΟΓΩΝ ΜΗΧΑΝΙΚΩΝ & ΜΗΧΑΝΙΚΩΝ ΥΠΟΛΟΓΙΣΤΩΝ")
  v(2em, weak : true)

  image("./ntua_logo.svg", width: 10em)
  v(4em, weak: true)

  text(20pt, weight: "bold", title)
  v(1em, weak: true)

  // Display semester.
  text(12pt, semester)
  v(3em, weak: true)

// Display subtitle
text(
  18pt, 
  fill: gradient.linear( 
    (rgb("#111194")),   // Dark Green
    (rgb("#66157e")),
    (rgb("#b67ed2dd")) // Lighter Green
  ), 
  style: "italic", 
  subtitle
)
v(1.8em, weak: true)


  // Author section.
  text(12pt, " Yλοποίηση από τους φοιτητές:")
  let count = authors.len()
  let ncols = calc.min(count, 2)
  grid(
    columns: (1fr,) * ncols,
    row-gutter: 10pt,
    ..authors.map(author => text(14pt, author))
  )
  v(2em, weak: true)

  // Team section.
  text(12pt, "Ομάδα: ")
  text(14pt,team)
  v(2em, weak: true)

  // Document body.
  set align(left)
  set page(numbering: "1", number-align: center)

  // Paragraph styling.
  set par(
    first-line-indent: 1em,
    spacing: 0.65em,
    justify: true,
  )

  // Display document content.
  doc
}
