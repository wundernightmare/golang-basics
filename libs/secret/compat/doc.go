// Package compat checks libs/secret against the configuration libraries it
// claims to work with — YAML, caarlos0/env, mapstructure (and with it viper).
//
// It is a module of its own so that those libraries are this suite's
// dependencies: libs/secret itself imports the standard library only, and its
// go.mod says so.
package compat
