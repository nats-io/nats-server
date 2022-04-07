// Copyright 2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestConfigCheck(t *testing.T) {
	tests := []struct {
		// name is the name of the test.
		name string

		// config is content of the configuration file.
		config string

		// warningErr is an error that does not prevent server from starting.
		warningErr error

		// errorLine is the location of the error.
		errorLine int

		// errorPos is the position of the error.
		errorPos int

		// warning errors also include a reason optionally.
		reason string

		// newDefaultErr is a configuration error that includes source of error.
		err error
	}{
		{
			name: "when unknown field is used at top level",
			config: `
                monitor = "127.0.0.1:4442"
                `,
			err:       errors.New(`unknown field "monitor"`),
			errorLine: 2,
			errorPos:  17,
		},
		{
			name: "when default permissions are used at top level",
			config: `
                "default_permissions" {
                  publish = ["_SANDBOX.>"]
                  subscribe = ["_SANDBOX.>"]
                }
                `,
			err:       errors.New(`unknown field "default_permissions"`),
			errorLine: 2,
			errorPos:  18,
		},
		{
			name: "when authorization config is empty",
			config: `
		authorization = {
		}
		`,
			err: nil,
		},
		{
			name: "when authorization config has unknown fields",
			config: `
		authorization = {
		  foo = "bar"
		}
		`,
			err:       errors.New(`unknown field "foo"`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when authorization config has unknown fields",
			config: `
		port = 4222

		authorization = {
		  user = "hello"
		  foo = "bar"
		  password = "world"
		}

		`,
			err:       errors.New(`unknown field "foo"`),
			errorLine: 6,
			errorPos:  5,
		},
		{
			name: "when user authorization config has unknown fields",
			config: `
		authorization = {
		  users = [
		    {
		      user = "foo"
		      pass = "bar"
		      token = "quux"
		    }
		  ]
		}
		`,
			err:       errors.New(`unknown field "token"`),
			errorLine: 7,
			errorPos:  9,
		},
		{
			name: "when user authorization permissions config has unknown fields",
			config: `
		authorization {
		  permissions {
		    subscribe = {}
		    inboxes = {}
		    publish = {}
		  }
		}
		`,
			err:       errors.New(`Unknown field "inboxes" parsing permissions`),
			errorLine: 5,
			errorPos:  7,
		},
		{
			name: "when user authorization permissions config has unknown fields within allow or deny",
			config: `
		authorization {
		  permissions {
		    subscribe = {
		      allow = ["hello", "world"]
		      deny = ["foo", "bar"]
		      denied = "_INBOX.>"
		    }
		    publish = {}
		  }
		}
		`,
			err:       errors.New(`Unknown field name "denied" parsing subject permissions, only 'allow' or 'deny' are permitted`),
			errorLine: 7,
			errorPos:  9,
		},
		{
			name: "when user authorization permissions config has unknown fields within allow or deny",
			config: `
		authorization {
		  permissions {
		    publish = {
		      allow = ["hello", "world"]
		      deny = ["foo", "bar"]
		      allowed = "_INBOX.>"
		    }
		    subscribe = {}
		  }
		}
		`,
			err:       errors.New(`Unknown field name "allowed" parsing subject permissions, only 'allow' or 'deny' are permitted`),
			errorLine: 7,
			errorPos:  9,
		},
		{
			name: "when user authorization permissions config has unknown fields using arrays",
			config: `
		authorization {

		 default_permissions {
		   subscribe = ["a"]
		   publish = ["b"]
		   inboxes = ["c"]
		 }

		 users = [
		   {
		     user = "foo"
		     pass = "bar"
		   }
		  ]
		}
		`,
			err:       errors.New(`Unknown field "inboxes" parsing permissions`),
			errorLine: 7,
			errorPos:  6,
		},
		{
			name: "when user authorization permissions config has unknown fields using strings",
			config: `
		authorization {

		 default_permissions {
		   subscribe = "a"
		   requests = "b"
		   publish = "c"
		 }

		 users = [
		   {
		     user = "foo"
		     pass = "bar"
		   }
		  ]
		}
		`,
			err:       errors.New(`Unknown field "requests" parsing permissions`),
			errorLine: 6,
			errorPos:  6,
		},
		{
			name: "when user authorization permissions config is empty",
			config: `
		authorization = {
		  users = [
		    {
		      user = "foo", pass = "bar", permissions = {
		      }
		    }
		  ]
		}
		`,
			err: nil,
		},
		{
			name: "when unknown permissions are included in user config",
			config: `
		authorization = {
		  users = [
		    {
		      user = "foo", pass = "bar", permissions {
		        inboxes = true
		      }
		    }
		  ]
		}
		`,
			err:       errors.New(`Unknown field "inboxes" parsing permissions`),
			errorLine: 6,
			errorPos:  11,
		},
		{
			name: "when clustering config is empty",
			config: `
		cluster = {
		}
		`,

			err: nil,
		},
		{
			name: "when unknown option is in clustering config",
			config: `
		# NATS Server Configuration
		port = 4222

		cluster = {

		  port = 6222

		  foo = "bar"

		  authorization {
		    user = "hello"
		    pass = "world"
		  }

		}
		`,

			err:       errors.New(`unknown field "foo"`),
			errorLine: 9,
			errorPos:  5,
		},
		{
			name: "when unknown option is in clustering authorization config",
			config: `
		cluster = {
		  authorization {
		    foo = "bar"
		  }
		}
		`,

			err:       errors.New(`unknown field "foo"`),
			errorLine: 4,
			errorPos:  7,
		},
		{
			name: "when unknown option is in tls config",
			config: `
		tls = {
		  hello = "world"
		}
		`,
			err:       errors.New(`error parsing tls config, unknown field ["hello"]`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when unknown option is in cluster tls config",
			config: `
		cluster {
		  tls = {
		    foo = "bar"
		  }
		}
		`,
			err:       errors.New(`error parsing tls config, unknown field ["foo"]`),
			errorLine: 4,
			errorPos:  7,
		},
		{
			name: "when using cipher suites in the TLS config",
			config: `
		tls = {
		    cipher_suites: [
			"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
		    ]
		    preferences = []
		}
		`,
			err:       errors.New(`error parsing tls config, unknown field ["preferences"]`),
			errorLine: 7,
			errorPos:  7,
		},
		{
			name: "when using curve preferences in the TLS config",
			config: `
		tls = {
		    curve_preferences: [
			"CurveP256",
			"CurveP384",
			"CurveP521"
		    ]
		    suites = []
		}
		`,
			err:       errors.New(`error parsing tls config, unknown field ["suites"]`),
			errorLine: 8,
			errorPos:  7,
		},
		{
			name: "when using curve preferences in the TLS config",
			config: `
		tls = {
		    curve_preferences: [
			"CurveP5210000"
		    ]
		}
		`,
			err:       errors.New(`unrecognized curve preference CurveP5210000`),
			errorLine: 4,
			errorPos:  5,
		},
		{
			name: "verify_cert_and_check_known_urls not support for clients",
			config: `
		tls = {
						cert_file: "configs/certs/server.pem"
						key_file: "configs/certs/key.pem"
					    verify_cert_and_check_known_urls: true
		}
		`,
			err:       errors.New("verify_cert_and_check_known_urls not supported in this context"),
			errorLine: 5,
			errorPos:  10,
		},
		{
			name: "when unknown option is in cluster config with defined routes",
			config: `
		cluster {
		  port = 6222
		  routes = [
		    nats://127.0.0.1:6222
		  ]
		  peers = []
		}
		`,
			err:       errors.New(`unknown field "peers"`),
			errorLine: 7,
			errorPos:  5,
		},
		{
			name: "when used as variable in authorization block it should not be considered as unknown field",
			config: `
		# listen:   127.0.0.1:-1
		listen:   127.0.0.1:4222

		authorization {
		  # Superuser can do anything.
		  super_user = {
		    publish = ">"
		    subscribe = ">"
		  }

		  # Can do requests on foo or bar, and subscribe to anything
		  # that is a response to an _INBOX.
		  #
		  # Notice that authorization filters can be singletons or arrays.
		  req_pub_user = {
		    publish = ["req.foo", "req.bar"]
		    subscribe = "_INBOX.>"
		  }

		  # Setup a default user that can subscribe to anything, but has
		  # no publish capabilities.
		  default_user = {
		    subscribe = "PUBLIC.>"
		  }

		  unused = "hello"

		  # Default permissions if none presented. e.g. susan below.
		  default_permissions: $default_user

		  # Users listed with persmissions.
		  users = [
		    {user: alice, password: foo, permissions: $super_user}
		    {user: bob,   password: bar, permissions: $req_pub_user}
		    {user: susan, password: baz}
		  ]
		}
		`,
			err:       errors.New(`unknown field "unused"`),
			errorLine: 27,
			errorPos:  5,
		},
		{
			name: "when used as variable in top level config it should not be considered as unknown field",
			config: `
		monitoring_port = 8222

		http_port = $monitoring_port

		port = 4222
		`,
			err: nil,
		},
		{
			name: "when used as variable in cluster config it should not be considered as unknown field",
			config: `
		cluster {
		  clustering_port = 6222
		  port = $clustering_port
		}
		`,
			err: nil,
		},
		{
			name: "when setting permissions within cluster authorization block",
			config: `
		cluster {
		  authorization {
		    permissions = {
		      publish = { allow = ["foo", "bar"] }
		    }
		  }

		  permissions = {
		    publish = { deny = ["foo", "bar"] }
		  }
		}
		`,
			warningErr: errors.New(`invalid use of field "authorization"`),
			errorLine:  3,
			errorPos:   5,
			reason:     `setting "permissions" within cluster authorization block is deprecated`,
		},
		{
			name: "when write deadline is used with deprecated usage",
			config: `
                write_deadline = 100
		`,
			warningErr: errors.New(`invalid use of field "write_deadline"`),
			errorLine:  2,
			errorPos:   17,
			reason:     `write_deadline should be converted to a duration`,
		},
		/////////////////////
		// ACCOUNTS	   //
		/////////////////////
		{
			name: "when accounts block is correctly configured",
			config: `
		http_port = 8222

		accounts {

		  #
		  # synadia > nats.io, cncf
		  #
		  synadia {
		    # SAADJL5XAEM6BDYSWDTGVILJVY54CQXZM5ZLG4FRUAKB62HWRTPNSGXOHA
		    nkey = "AC5GRL36RQV7MJ2GT6WQSCKDKJKYTK4T2LGLWJ2SEJKRDHFOQQWGGFQL"

		    users [
		      {
		        # SUAEL6RU3BSDAFKOHNTEOK5Q6FTM5FTAMWVIKBET6FHPO4JRII3CYELVNM
		        nkey = "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"
		      }
		    ]

		    exports = [
		      { service: "synadia.requests", accounts: [nats, cncf] }
		    ]
		  }

		  #
		  # nats < synadia
		  #
		  nats {
		    # SUAJTM55JH4BNYDA22DMDZJSRBRKVDGSLYK2HDIOCM3LPWCDXIDV5Q4CIE
		    nkey = "ADRZ42QBM7SXQDXXTSVWT2WLLFYOQGAFC4TO6WOAXHEKQHIXR4HFYJDS"

		    users [
		      {
		        # SUADZTYQAKTY5NQM7XRB5XR3C24M6ROGZLBZ6P5HJJSSOFUGC5YXOOECOM
		        nkey = "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"
		      }
		    ]

		    imports = [
		      # This account has to send requests to 'nats.requests' subject
		      { service: { account: "synadia", subject: "synadia.requests" }, to: "nats.requests" }
		    ]
		  }

		  #
		  # cncf < synadia
		  #
		  cncf {
		    # SAAFHDZX7SGZ2SWHPS22JRPPK5WX44NPLNXQHR5C5RIF6QRI3U65VFY6C4
		    nkey = "AD4YRVUJF2KASKPGRMNXTYKIYSCB3IHHB4Y2ME6B2PDIV5QJ23C2ZRIT"

		    users [
		      {
		        # SUAKINP3Z2BPUXWOFSW2FZC7TFJCMMU7DHKP2C62IJQUDASOCDSTDTRMJQ
		        nkey = "UB57IEMPG4KOTPFV5A66QKE2HZ3XBXFHVRCCVMJEWKECMVN2HSH3VTSJ"
		      }
		    ]

		    imports = [
		      # This account has to send requests to 'synadia.requests' subject
		      { service: { account: "synadia", subject: "synadia.requests" } }
		    ]
		  }
		}
				`,
			err: nil,
		},
		{
			name: "when nkey is invalid within accounts block",
			config: `
		accounts {

		  #
		  # synadia > nats.io, cncf
		  #
		  synadia {
		    # SAADJL5XAEM6BDYSWDTGVILJVY54CQXZM5ZLG4FRUAKB62HWRTPNSGXOHA
		    nkey = "AC5GRL36RQV7MJ2GT6WQSCKDKJKYTK4T2LGLWJ2SEJKRDHFOQQWGGFQL"

		    users [
		      {
		        # SUAEL6RU3BSDAFKOHNTEOK5Q6FTM5FTAMWVIKBET6FHPO4JRII3CYELVNM
		        nkey = "SCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"
		      }
		    ]

		    exports = [
		      { service: "synadia.requests", accounts: [nats, cncf] }
		    ]
		  }

		  #
		  # nats < synadia
		  #
		  nats {
		    # SUAJTM55JH4BNYDA22DMDZJSRBRKVDGSLYK2HDIOCM3LPWCDXIDV5Q4CIE
		    nkey = "ADRZ42QBM7SXQDXXTSVWT2WLLFYOQGAFC4TO6WOAXHEKQHIXR4HFYJDS"

		    users [
		      {
		        # SUADZTYQAKTY5NQM7XRB5XR3C24M6ROGZLBZ6P5HJJSSOFUGC5YXOOECOM
		        nkey = "UD6AYQSOIN2IN5OGC6VQZCR4H3UFMIOXSW6NNS6N53CLJA4PB56CEJJI"
		      }
		    ]

		    imports = [
		      # This account has to send requests to 'nats.requests' subject
		      { service: { account: "synadia", subject: "synadia.requests" }, to: "nats.requests" }
		    ]
		  }

		  #
		  # cncf < synadia
		  #
		  cncf {
		    # SAAFHDZX7SGZ2SWHPS22JRPPK5WX44NPLNXQHR5C5RIF6QRI3U65VFY6C4
		    nkey = "AD4YRVUJF2KASKPGRMNXTYKIYSCB3IHHB4Y2ME6B2PDIV5QJ23C2ZRIT"

		    users [
		      {
		        # SUAKINP3Z2BPUXWOFSW2FZC7TFJCMMU7DHKP2C62IJQUDASOCDSTDTRMJQ
		        nkey = "UB57IEMPG4KOTPFV5A66QKE2HZ3XBXFHVRCCVMJEWKECMVN2HSH3VTSJ"
		      }
		    ]

		    imports = [
		      # This account has to send requests to 'synadia.requests' subject
		      { service: { account: "synadia", subject: "synadia.requests" } }
		    ]
		  }
		}
				`,
			err:       errors.New(`Not a valid public nkey for a user`),
			errorLine: 14,
			errorPos:  11,
		},
		{
			name: "when accounts block has unknown fields",
			config: `
		http_port = 8222

		accounts {
                  foo = "bar"
		}`,
			err:       errors.New(`Expected map entries for accounts`),
			errorLine: 5,
			errorPos:  19,
		},
		{
			name: "when accounts has a referenced config variable within same block",
			config: `
			  accounts {
			    PERMISSIONS = {
				publish = {
				  allow = ["foo","bar"]
				  deny = ["quux"]
				}
			    }

			    synadia {
				nkey = "AC5GRL36RQV7MJ2GT6WQSCKDKJKYTK4T2LGLWJ2SEJKRDHFOQQWGGFQL"

				users [
				  {
				    nkey = "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"
				    permissions = $PERMISSIONS
				  }
				]
				exports = [
				  { stream: "synadia.>" }
				]
			    }
			  }`,
			err: nil,
		},
		{
			name: "when accounts has an unreferenced config variables within same block",
			config: `
			  accounts {
			    PERMISSIONS = {
				publish = {
				  allow = ["foo","bar"]
				  deny = ["quux"]
				}
			    }

			    synadia {
				nkey = "AC5GRL36RQV7MJ2GT6WQSCKDKJKYTK4T2LGLWJ2SEJKRDHFOQQWGGFQL"

				users [
				  {
				    nkey = "UCARKS2E3KVB7YORL2DG34XLT7PUCOL2SVM7YXV6ETHLW6Z46UUJ2VZ3"
				  }
				]
				exports = [
				  { stream: "synadia.>" }
				]
			   }
			 }`,
			err:       errors.New(`unknown field "publish"`),
			errorLine: 4,
			errorPos:  5,
		},
		{
			name: "when accounts block defines a global account",
			config: `
		http_port = 8222

		accounts {
                  $G = {
                  }
		}
				`,
			err:       errors.New(`"$G" is a Reserved Account`),
			errorLine: 5,
			errorPos:  19,
		},
		{
			name: "when accounts block uses an invalid public key",
			config: `
		accounts {
                  synadia = {
                    nkey = "invalid"
                  }
		}
				`,
			err:       errors.New(`Not a valid public nkey for an account: "invalid"`),
			errorLine: 4,
			errorPos:  21,
		},
		{
			name: "when accounts list includes reserved account",
			config: `
                port = 4222

		accounts = [foo, bar, "$G"]

                http_port = 8222
				`,
			err:       errors.New(`"$G" is a Reserved Account`),
			errorLine: 4,
			errorPos:  26,
		},
		{
			name: "when accounts list includes a dupe entry",
			config: `
                port = 4222

		accounts = [foo, bar, bar]

                http_port = 8222
				`,
			err:       errors.New(`Duplicate Account Entry: bar`),
			errorLine: 4,
			errorPos:  25,
		},
		{
			name: "when accounts block includes a dupe user",
			config: `
                port = 4222

		accounts = {
                  nats {
                    users = [
                      { user: "foo",   pass: "bar" },
                      { user: "hello", pass: "world" },
                      { user: "foo",   pass: "bar" }
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Duplicate user "foo" detected`),
			errorLine: 6,
			errorPos:  21,
		},
		{
			name: "when accounts block imports are not a list",
			config: `
                port = 4222

		accounts = {
                  nats {
                    imports = true
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Imports should be an array, got bool`),
			errorLine: 6,
			errorPos:  21,
		},
		{
			name: "when accounts block exports are not a list",
			config: `
                port = 4222

		accounts = {
                  nats {
                    exports = true
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Exports should be an array, got bool`),
			errorLine: 6,
			errorPos:  21,
		},
		{
			name: "when accounts block imports items are not a map",
			config: `
                port = 4222

		accounts = {
                  nats {
                    imports = [
                      false
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Import Items should be a map with type entry, got bool`),
			errorLine: 7,
			errorPos:  23,
		},
		{
			name: "when accounts block export items are not a map",
			config: `
                port = 4222

		accounts = {
                  nats {
                    exports = [
                      false
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Export Items should be a map with type entry, got bool`),
			errorLine: 7,
			errorPos:  23,
		},
		{
			name: "when accounts exports has a stream name that is not a string",
			config: `
                port = 4222

		accounts = {
                  nats {
                    exports = [
                      {
                        stream: false
                      }
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Expected stream name to be string, got bool`),
			errorLine: 8,
			errorPos:  25,
		},
		{
			name: "when accounts exports has a service name that is not a string",
			config: `
		accounts = {
                  nats {
                    exports = [
                      {
                        service: false
                      }
                    ]
                  }
                }
				`,
			err:       errors.New(`Expected service name to be string, got bool`),
			errorLine: 6,
			errorPos:  25,
		},
		{
			name: "when accounts imports stream without name",
			config: `
                port = 4222

		accounts = {
                  nats {
                    imports = [
                      { stream: { }}
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Expect an account name and a subject`),
			errorLine: 7,
			errorPos:  25,
		},
		{
			name: "when accounts imports service without name",
			config: `
                port = 4222

		accounts = {
                  nats {
                    imports = [
                      { service: { }}
                    ]
                  }
                }

                http_port = 8222
				`,
			err:       errors.New(`Expect an account name and a subject`),
			errorLine: 7,
			errorPos:  25,
		},
		{
			name: "when user authorization config has both token and users",
			config: `
		authorization = {
			token = "s3cr3t"
			users = [
				{
					user = "foo"
					pass = "bar"
				}
			]
		}
		`,
			err:       errors.New(`Can not have a token and a users array`),
			errorLine: 2,
			errorPos:  3,
		},
		{
			name: "when user authorization config has both token and user",
			config: `
		authorization = {
			user = "foo"
			pass = "bar"
			token = "baz"
		}
		`,
			err:       errors.New(`Cannot have a user/pass and token`),
			errorLine: 2,
			errorPos:  3,
		},
		{
			name: "when user authorization config has both user and users array",
			config: `
		authorization = {
			user = "user1"
			pass = "pwd1"
			users = [
				{
					user = "user2"
					pass = "pwd2"
				}
			]
		}
		`,
			err:       errors.New(`Can not have a single user/pass and a users array`),
			errorLine: 2,
			errorPos:  3,
		},
		{
			name: "when user authorization has duplicate users",
			config: `
		authorization = {
			users = [
				{user: "user1", pass: "pwd"}
				{user: "user2", pass: "pwd"}
				{user: "user1", pass: "pwd"}
			]
		}
		`,
			err:       fmt.Errorf(`Duplicate user %q detected`, "user1"),
			errorLine: 2,
			errorPos:  3,
		},
		{
			name: "when user authorization has duplicate nkeys",
			config: `
		authorization = {
			users = [
				{nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX }
				{nkey: UBAAQWTW6CG2G6ANGNKB5U2B7HRWHSGMZEZX3AQSAJOQDAUGJD46LD2E }
				{nkey: UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX }
			]
		}
		`,
			err:       fmt.Errorf(`Duplicate nkey %q detected`, "UC6NLCN7AS34YOJVCYD4PJ3QB7QGLYG5B5IMBT25VW5K4TNUJODM7BOX"),
			errorLine: 2,
			errorPos:  3,
		},
		{
			name: "when user authorization config has users not as a list",
			config: `
		authorization = {
		  users = false
		}
		`,
			err:       errors.New(`Expected users field to be an array, got false`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when user authorization config has users not as a map",
			config: `
		authorization = {
		  users = [false]
		}
		`,
			err:       errors.New(`Expected user entry to be a map/struct, got false`),
			errorLine: 3,
			errorPos:  14,
		},
		{
			name: "when user authorization config has permissions not as a map",
			config: `
		authorization = {
		  users = [{user: hello, pass: world}]
                  permissions = false
		}
		`,
			err:       errors.New(`Expected permissions to be a map/struct, got false`),
			errorLine: 4,
			errorPos:  19,
		},
		{
			name: "when user authorization permissions config has invalid fields within allow",
			config: `
		authorization {
		  permissions {
		    publish = {
		      allow = [false, "hello", "world"]
		      deny = ["foo", "bar"]
		    }
		    subscribe = {}
		  }
		}
		`,
			err:       errors.New(`Subject in permissions array cannot be cast to string`),
			errorLine: 5,
			errorPos:  18,
		},
		{
			name: "when user authorization permissions config has invalid fields within deny",
			config: `
		authorization {
		  permissions {
		    publish = {
		      allow = ["hello", "world"]
		      deny = [true, "foo", "bar"]
		    }
		    subscribe = {}
		  }
		}
		`,
			err:       errors.New(`Subject in permissions array cannot be cast to string`),
			errorLine: 6,
			errorPos:  17,
		},
		{
			name: "when user authorization permissions config has invalid type",
			config: `
		authorization {
		  permissions {
		    publish = {
		      allow = false
		    }
		    subscribe = {}
		  }
		}
		`,
			err:       errors.New(`Expected subject permissions to be a subject, or array of subjects, got bool`),
			errorLine: 5,
			errorPos:  9,
		},
		{
			name: "when user authorization permissions subject is invalid",
			config: `
		authorization {
		  permissions {
		    publish = {
		      allow = ["foo..bar"]
		    }
		    subscribe = {}
		  }
		}
		`,
			err:       errors.New(`subject "foo..bar" is not a valid subject`),
			errorLine: 5,
			errorPos:  9,
		},
		{
			name: "when cluster config listen is invalid",
			config: `
		cluster {
		  listen = "0.0.0.0:XXXX"
		}
		`,
			err:       errors.New(`could not parse port "XXXX"`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when cluster config includes multiple users",
			config: `
		cluster {
		  authorization {
                    users = []
                  }
		}
		`,
			err:       errors.New(`Cluster authorization does not allow multiple users`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when cluster routes are invalid",
			config: `
		cluster {
                  routes = [
                    "0.0.0.0:XXXX"
                    # "0.0.0.0:YYYY"
                    # "0.0.0.0:ZZZZ"
                  ]
		}
		`,
			err:       errors.New(`error parsing route url ["0.0.0.0:XXXX"]`),
			errorLine: 4,
			errorPos:  22,
		},
		{
			name: "when setting invalid TLS config within cluster block",
			config: `
		cluster {
		  tls {
		  }
		}
		`,
			err:       nil,
			errorLine: 0,
			errorPos:  0,
		},
		{
			name: "invalid lame_duck_duration type",
			config: `
				lame_duck_duration: abc
			`,
			err:       errors.New(`error parsing lame_duck_duration: time: invalid duration`),
			errorLine: 2,
			errorPos:  5,
		},
		{
			name: "lame_duck_duration too small",
			config: `
				lame_duck_duration: "5s"
			`,
			err:       errors.New(`invalid lame_duck_duration of 5s, minimum is 30 seconds`),
			errorLine: 2,
			errorPos:  5,
		},
		{
			name: "invalid lame_duck_grace_period type",
			config: `
				lame_duck_grace_period: abc
			`,
			err:       errors.New(`error parsing lame_duck_grace_period: time: invalid duration`),
			errorLine: 2,
			errorPos:  5,
		},
		{
			name: "lame_duck_grace_period should be positive",
			config: `
				lame_duck_grace_period: "-5s"
			`,
			err:       errors.New(`invalid lame_duck_grace_period, needs to be positive`),
			errorLine: 2,
			errorPos:  5,
		},
		{
			name: "when only setting TLS timeout for a leafnode remote",
			config: `
		leafnodes {
		  remotes = [
		    {
		      url: "tls://nats:7422"
		      tls {
		        timeout: 0.01
		      }
		    }
		  ]
		}`,
			err:       nil,
			errorLine: 0,
			errorPos:  0,
		},
		{
			name: "verify_cert_and_check_known_urls do not work for leaf nodes",
			config: `
		leafnodes {
		  remotes = [
		    {
		      url: "tls://nats:7422"
		      tls {
		        timeout: 0.01
				verify_cert_and_check_known_urls: true
		      }
		    }
		  ]
		}`,
			//Unexpected error after processing config: /var/folders/9h/6g_c9l6n6bb8gp331d_9y0_w0000gn/T/057996446:8:5:
			err:       errors.New("verify_cert_and_check_known_urls not supported in this context"),
			errorLine: 8,
			errorPos:  5,
		},
		{
			name: "when leafnode remotes use wrong type",
			config: `
		leafnodes {
		  remotes: {
  	            url: "tls://nats:7422"
		  }
		}`,
			err:       errors.New(`Expected remotes field to be an array, got map[string]interface {}`),
			errorLine: 3,
			errorPos:  5,
		},
		{
			name: "when leafnode remotes url uses wrong type",
			config: `
		leafnodes {
		  remotes: [
  	            { urls: 1234 }
		  ]
		}`,
			err:       errors.New(`Expected remote leafnode url to be an array or string, got 1234`),
			errorLine: 4,
			errorPos:  18,
		},
		{
			name: "when leafnode min_version is wrong type",
			config: `
				leafnodes {
					port: -1
					min_version = 123
				}`,
			err:       errors.New(`interface conversion: interface {} is int64, not string`),
			errorLine: 4,
			errorPos:  6,
		},
		{
			name: "when leafnode min_version has parsing error",
			config: `
				leafnodes {
					port: -1
					min_version = bad.version
				}`,
			err:       errors.New(`invalid leafnode's minimum version: invalid semver`),
			errorLine: 4,
			errorPos:  6,
		},
		{
			name: "when leafnode min_version is too low",
			config: `
				leafnodes {
					port: -1
					min_version = 2.7.9
				}`,
			err:       errors.New(`the minimum version should be at least 2.8.0`),
			errorLine: 4,
			errorPos:  6,
		},
		{
			name: "when setting latency tracking with a system account",
			config: `
                system_account: sys

                accounts {
                  sys { users = [ {user: sys, pass: "" } ] }

                  nats.io: {
                    users = [ { user : bar, pass: "" } ]

                    exports = [
                      { service: "nats.add"
                        response: singleton
                        latency: {
                          sampling: 100%
                          subject: "latency.tracking.add"
                        }
                      }

                    ]
                  }
                }
                `,
			err:       nil,
			errorLine: 0,
			errorPos:  0,
		},
		{
			name: "when setting latency tracking with an invalid publish subject",
			config: `
                system_account = sys
                accounts {
                  sys { users = [ {user: sys, pass: "" } ] }

                  nats.io: {
                    users = [ { user : bar, pass: "" } ]

                    exports = [
                      { service: "nats.add"
                        response: singleton
                        latency: "*"
                      }
                    ]
                  }
                }
                `,
			err:       errors.New(`Error adding service latency sampling for "nats.add" on subject "*": invalid publish subject`),
			errorLine: 3,
			errorPos:  17,
		},
		{
			name: "when setting latency tracking on a stream",
			config: `
                system_account = sys
                accounts {
                  sys { users = [ {user: sys, pass: "" } ] }

                  nats.io: {
                    users = [ { user : bar, pass: "" } ]

                    exports = [
                      { stream: "nats.add"
                        latency: "foo"
                      }
                    ]
                  }
                }
                `,
			err:       errors.New(`Detected latency directive on non-service`),
			errorLine: 11,
			errorPos:  25,
		},
		{
			name: "when using duplicate service import subject",
			config: `
								accounts {
									 A: {
										 users = [ {user: user1, pass: ""} ]
										 exports = [
											 {service: "remote1"}
											 {service: "remote2"}
										 ]
									 }
									 B: {
										 users = [ {user: user2, pass: ""} ]
										 imports = [
											 {service: {account: "A", subject: "remote1"}, to: "local"}
											 {service: {account: "A", subject: "remote2"}, to: "local"}
										 ]
									 }
								}
							`,
			err:       errors.New(`Duplicate service import subject "local", previously used in import for account "A", subject "remote1"`),
			errorLine: 14,
			errorPos:  71,
		},
		{
			name: "mixing single and multi users in leafnode authorization",
			config: `
                leafnodes {
                   authorization {
                     user: user1
                     password: pwd
                     users = [{user: user2, password: pwd}]
                   }
                }
              `,
			err:       errors.New("can not have a single user/pass and a users array"),
			errorLine: 3,
			errorPos:  20,
		},
		{
			name: "duplicate usernames in leafnode authorization",
			config: `
                leafnodes {
                    authorization {
                        users = [
                            {user: user, password: pwd}
                            {user: user, password: pwd}
                        ]
                    }
                }
              `,
			err:       errors.New(`duplicate user "user" detected in leafnode authorization`),
			errorLine: 3,
			errorPos:  21,
		},
		{
			name: "mqtt bad type",
			config: `
                mqtt [
					"wrong"
				]
			`,
			err:       errors.New(`Expected mqtt to be a map, got []interface {}`),
			errorLine: 2,
			errorPos:  17,
		},
		{
			name: "mqtt bad listen",
			config: `
                mqtt {
                    listen: "xxxxxxxx"
				}
			`,
			err:       errors.New(`could not parse address string "xxxxxxxx"`),
			errorLine: 3,
			errorPos:  21,
		},
		{
			name: "mqtt bad host",
			config: `
                mqtt {
                    host: 1234
				}
			`,
			err:       errors.New(`interface conversion: interface {} is int64, not string`),
			errorLine: 3,
			errorPos:  21,
		},
		{
			name: "mqtt bad port",
			config: `
                mqtt {
                    port: "abc"
				}
			`,
			err:       errors.New(`interface conversion: interface {} is string, not int64`),
			errorLine: 3,
			errorPos:  21,
		},
		{
			name: "mqtt bad TLS",
			config: `
                mqtt {
					port: -1
                    tls {
                        cert_file: "./configs/certs/server.pem"
					}
				}
			`,
			err:       errors.New(`missing 'key_file' in TLS configuration`),
			errorLine: 4,
			errorPos:  21,
		},
		{
			name: "connection types wrong type",
			config: `
                   authorization {
                       users [
                           {user: a, password: pwd, allowed_connection_types: 123}
					   ]
				   }
			`,
			err:       errors.New(`error parsing allowed connection types: unsupported type int64`),
			errorLine: 4,
			errorPos:  53,
		},
		{
			name: "connection types content wrong type",
			config: `
                   authorization {
                       users [
                           {user: a, password: pwd, allowed_connection_types: [
                               123
                               WEBSOCKET
							]}
					   ]
				   }
			`,
			err:       errors.New(`error parsing allowed connection types: unsupported type in array int64`),
			errorLine: 5,
			errorPos:  32,
		},
		{
			name: "connection types type unknown",
			config: `
                   authorization {
                       users [
                           {user: a, password: pwd, allowed_connection_types: [ "UNKNOWN" ]}
					   ]
				   }
			`,
			err:       fmt.Errorf("invalid connection types [%q]", "UNKNOWN"),
			errorLine: 4,
			errorPos:  53,
		},
		{
			name: "websocket auth unknown var",
			config: `
				websocket {
					authorization {
                        unknown: "field"
				   }
				}
			`,
			err:       fmt.Errorf("unknown field %q", "unknown"),
			errorLine: 4,
			errorPos:  25,
		},
		{
			name: "websocket bad tls",
			config: `
				websocket {
                    tls {
						cert_file: "configs/certs/server.pem"
					}
				}
			`,
			err:       fmt.Errorf("missing 'key_file' in TLS configuration"),
			errorLine: 3,
			errorPos:  21,
		},
		{
			name: "verify_cert_and_check_known_urls not support for websockets",
			config: `
				websocket {
                    tls {
						cert_file: "configs/certs/server.pem"
						key_file: "configs/certs/key.pem"
					    verify_cert_and_check_known_urls: true
					}
				}
			`,
			err:       fmt.Errorf("verify_cert_and_check_known_urls not supported in this context"),
			errorLine: 6,
			errorPos:  10,
		},
		{
			name: "ambiguous store dir",
			config: `
                                store_dir: "foo"
                                jetstream {
                                  store_dir: "bar"
                                }
                        `,
			err: fmt.Errorf(`Duplicate 'store_dir' configuration`),
		},
		{
			name: "token not supported in cluster",
			config: `
				cluster {
					port: -1
					authorization {
						token: "my_token"
					}
				}
			`,
			err:       fmt.Errorf("Cluster authorization does not support tokens"),
			errorLine: 4,
			errorPos:  6,
		},
		{
			name: "token not supported in gateway",
			config: `
				gateway {
					port: -1
					name: "A"
					authorization {
						token: "my_token"
					}
				}
			`,
			err:       fmt.Errorf("Gateway authorization does not support tokens"),
			errorLine: 5,
			errorPos:  6,
		},
	}

	checkConfig := func(config string) error {
		opts := &Options{
			CheckConfig: true,
		}
		return opts.ProcessConfigFile(config)
	}

	checkErr := func(t *testing.T, err, expectedErr error) {
		t.Helper()
		switch {
		case err == nil && expectedErr == nil:
			// OK
		case err != nil && expectedErr == nil:
			t.Errorf("Unexpected error after processing config: %s", err)
		case err == nil && expectedErr != nil:
			t.Errorf("Expected %q error after processing invalid config but got nothing", expectedErr)
		}
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf := createConfFile(t, []byte(test.config))
			defer removeFile(t, conf)
			err := checkConfig(conf)
			var expectedErr error

			// Check for either warnings or errors.
			if test.err != nil {
				expectedErr = test.err
			} else if test.warningErr != nil {
				expectedErr = test.warningErr
			}

			if err != nil && expectedErr != nil {
				var msg string

				if test.errorPos > 0 {
					msg = fmt.Sprintf("%s:%d:%d: %s", conf, test.errorLine, test.errorPos, expectedErr.Error())
					if test.reason != "" {
						msg += ": " + test.reason
					}
				} else {
					msg = test.reason
				}

				if !strings.Contains(err.Error(), msg) {
					t.Errorf("Expected:\n%q\ngot:\n%q", msg, err.Error())
				}
			}

			checkErr(t, err, expectedErr)
		})
	}
}

func TestConfigCheckIncludes(t *testing.T) {
	// Check happy path first.
	opts := &Options{
		CheckConfig: true,
	}
	err := opts.ProcessConfigFile("./configs/include_conf_check_a.conf")
	if err != nil {
		t.Errorf("Unexpected error processing include files with configuration check enabled: %v", err)
	}

	opts = &Options{
		CheckConfig: true,
	}
	err = opts.ProcessConfigFile("./configs/include_bad_conf_check_a.conf")
	if err == nil {
		t.Errorf("Expected error processing include files with configuration check enabled: %v", err)
	}
	expectedErr := `include_bad_conf_check_b.conf:10:19: unknown field "monitoring_port"` + "\n"
	if err != nil && !strings.HasSuffix(err.Error(), expectedErr) {
		t.Errorf("Expected: \n%q, got\n: %q", expectedErr, err.Error())
	}
}

func TestConfigCheckMultipleErrors(t *testing.T) {
	opts := &Options{
		CheckConfig: true,
	}
	err := opts.ProcessConfigFile("./configs/multiple_errors.conf")
	if err == nil {
		t.Errorf("Expected error processing config files with multiple errors check enabled: %v", err)
	}
	cerr, ok := err.(*processConfigErr)
	if !ok {
		t.Fatalf("Expected a configuration process error")
	}
	got := len(cerr.Warnings())
	expected := 1
	if got != expected {
		t.Errorf("Expected a %d warning, got: %d", expected, got)
	}
	got = len(cerr.Errors())
	// Could be 7 or 8 errors depending on internal ordering of the parsing.
	if got != 7 && got != 8 {
		t.Errorf("Expected 7 or 8 errors, got: %d", got)
	}

	errMsg := err.Error()

	errs := []string{
		`./configs/multiple_errors.conf:12:1: invalid use of field "write_deadline": write_deadline should be converted to a duration`,
		`./configs/multiple_errors.conf:2:1: Cannot have a user/pass and token`,
		`./configs/multiple_errors.conf:10:1: unknown field "monitoring"`,
		`./configs/multiple_errors.conf:67:3: Cluster authorization does not allow multiple users`,
		`./configs/multiple_errors.conf:21:5: Not a valid public nkey for an account: "OC5GRL36RQV7MJ2GT6WQSCKDKJKYTK4T2LGLWJ2SEJKRDHFOQQWGGFQL"`,
		`./configs/multiple_errors.conf:26:9: Not a valid public nkey for a user`,
		`./configs/multiple_errors.conf:36:5: Not a valid public nkey for an account: "ODRZ42QBM7SXQDXXTSVWT2WLLFYOQGAFC4TO6WOAXHEKQHIXR4HFYJDS"`,
		`./configs/multiple_errors.conf:41:9: Not a valid public nkey for a user`,
	}
	for _, msg := range errs {
		found := strings.Contains(errMsg, msg)
		if !found {
			t.Fatalf("Expected to find error %q", msg)
		}
	}
	if got == 8 {
		extra := "./configs/multiple_errors.conf:54:5: Can not have a single user/pass and accounts"
		if !strings.Contains(errMsg, extra) {
			t.Fatalf("Expected to find error %q (%s)", extra, errMsg)
		}
	}
}
