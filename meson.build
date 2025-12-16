project(
  'odbc.f',
  'fortran',
  'cpp',
  version: '1.0.0',
)

install = not (meson.is_subproject() and get_option('default_library') == 'static')

odbc_dep = dependency('odbc', required: true)
inc_dir = include_directories('include')

srcs = files(
  'src/columnset.F90',
  'src/connection.F90',
  'src/constants.F90',
  'src/odbc.F90',
  'src/resultset.F90',
  'src/sqlext.f90',
  'src/sql.f90',
  'src/sqltypes.f90',
)

odbcf_lib = static_library(
  meson.project_name(),
  sources: srcs,
  install: install,
  include_directories: inc_dir,
  dependencies: odbc_dep,
)

pkg = import('pkgconfig')
pkg.generate(
  odbcf_lib,
  description: 'odbc wrapper for modern Fortran',
)

# Export odbc.f as a Dependency for Other Projects
odbcf_inc = odbcf_lib.private_dir_include()
odbcf_dep = declare_dependency(
  link_with: odbcf_lib,
  include_directories: odbcf_inc,
)
