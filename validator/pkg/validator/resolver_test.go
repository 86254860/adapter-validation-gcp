package validator_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"validator/pkg/validator"
)

var _ = Describe("DependencyResolver", func() {
	var (
		resolver   *validator.DependencyResolver
		validators []validator.Validator
	)

	Describe("ResolveExecutionGroups", func() {
		Context("with validators that have no dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "validator-a",
						runAfter: []string{},
						enabled:  true,
					},
					&MockValidator{
						name:     "validator-b",
						runAfter: []string{},
						enabled:  true,
					},
					&MockValidator{
						name:     "validator-c",
						runAfter: []string{},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should place all validators in level 0", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				Expect(groups).To(HaveLen(1))
				Expect(groups[0].Level).To(Equal(0))
				Expect(groups[0].Validators).To(HaveLen(3))
			})

			It("should sort validators alphabetically within the same level", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				names := make([]string, len(groups[0].Validators))
				for i, v := range groups[0].Validators {
					names[i] = v.Metadata().Name
				}
				Expect(names).To(Equal([]string{"validator-a", "validator-b", "validator-c"}))
			})
		})

		Context("with linear dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "validator-a",
						runAfter: []string{},
						enabled:  true,
					},
					&MockValidator{
						name:     "validator-b",
						runAfter: []string{"validator-a"},
						enabled:  true,
					},
					&MockValidator{
						name:     "validator-c",
						runAfter: []string{"validator-b"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should create separate levels for each validator", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				Expect(groups).To(HaveLen(3))

				Expect(groups[0].Level).To(Equal(0))
				Expect(groups[0].Validators).To(HaveLen(1))
				Expect(groups[0].Validators[0].Metadata().Name).To(Equal("validator-a"))

				Expect(groups[1].Level).To(Equal(1))
				Expect(groups[1].Validators).To(HaveLen(1))
				Expect(groups[1].Validators[0].Metadata().Name).To(Equal("validator-b"))

				Expect(groups[2].Level).To(Equal(2))
				Expect(groups[2].Validators).To(HaveLen(1))
				Expect(groups[2].Validators[0].Metadata().Name).To(Equal("validator-c"))
			})
		})

		Context("with parallel dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "wif-check",
						runAfter: []string{},
						enabled:  true,
					},
					&MockValidator{
						name:     "api-enabled",
						runAfter: []string{"wif-check"},
						enabled:  true,
					},
					&MockValidator{
						name:     "quota-check",
						runAfter: []string{"wif-check"},
						enabled:  true,
					},
					&MockValidator{
						name:     "network-check",
						runAfter: []string{"wif-check"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should group validators with same dependencies at the same level", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				Expect(groups).To(HaveLen(2))

				// Level 0: wif-check
				Expect(groups[0].Level).To(Equal(0))
				Expect(groups[0].Validators).To(HaveLen(1))
				Expect(groups[0].Validators[0].Metadata().Name).To(Equal("wif-check"))

				// Level 1: api-enabled, quota-check, network-check (parallel)
				Expect(groups[1].Level).To(Equal(1))
				Expect(groups[1].Validators).To(HaveLen(3))
				names := make([]string, 3)
				for i, v := range groups[1].Validators {
					names[i] = v.Metadata().Name
				}
				Expect(names).To(ConsistOf("api-enabled", "quota-check", "network-check"))
			})
		})

		Context("with complex dependency graph", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "wif-check",
						runAfter: []string{},
						enabled:  true,
					},
					&MockValidator{
						name:     "api-enabled",
						runAfter: []string{"wif-check"},
						enabled:  true,
					},
					&MockValidator{
						name:     "quota-check",
						runAfter: []string{"wif-check"},
						enabled:  true,
					},
					&MockValidator{
						name:     "iam-check",
						runAfter: []string{"api-enabled"},
						enabled:  true,
					},
					&MockValidator{
						name:     "network-check",
						runAfter: []string{"api-enabled", "quota-check"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should create correct levels based on dependencies", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				Expect(groups).To(HaveLen(3))

				// Level 0: wif-check
				Expect(groups[0].Level).To(Equal(0))
				Expect(groups[0].Validators[0].Metadata().Name).To(Equal("wif-check"))

				// Level 1: api-enabled, quota-check
				Expect(groups[1].Level).To(Equal(1))
				Expect(groups[1].Validators).To(HaveLen(2))

				// Level 2: iam-check, network-check
				Expect(groups[2].Level).To(Equal(2))
				Expect(groups[2].Validators).To(HaveLen(2))
			})
		})

		Context("with circular dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "validator-a",
						runAfter: []string{"validator-b"},
						enabled:  true,
					},
					&MockValidator{
						name:     "validator-b",
						runAfter: []string{"validator-a"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should detect the circular dependency and return an error", func() {
				_, err := resolver.ResolveExecutionGroups()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("circular dependency"))
			})
		})

		Context("with self-referencing dependency", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "validator-a",
						runAfter: []string{"validator-a"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should detect the circular dependency and return an error", func() {
				_, err := resolver.ResolveExecutionGroups()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("circular dependency"))
			})
		})

		Context("with missing dependency", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{
						name:     "validator-a",
						runAfter: []string{"non-existent"},
						enabled:  true,
					},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should handle missing dependencies gracefully", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				// Missing dependencies are ignored, validator runs at level 0
				Expect(groups).To(HaveLen(1))
				Expect(groups[0].Level).To(Equal(0))
			})
		})

		Context("with empty validator list", func() {
			BeforeEach(func() {
				validators = []validator.Validator{}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should return empty groups", func() {
				groups, err := resolver.ResolveExecutionGroups()
				Expect(err).NotTo(HaveOccurred())
				Expect(groups).To(BeEmpty())
			})
		})
	})

	Describe("ToMermaid", func() {
		Context("with validators that have no dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "validator-a", runAfter: []string{}, enabled: true},
					&MockValidator{name: "validator-b", runAfter: []string{}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render standalone nodes", func() {
				mermaid := resolver.ToMermaid()
				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("validator-a"))
				Expect(mermaid).To(ContainSubstring("validator-b"))
				Expect(mermaid).NotTo(ContainSubstring("-->"))
			})
		})

		Context("with linear dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "validator-a", runAfter: []string{}, enabled: true},
					&MockValidator{name: "validator-b", runAfter: []string{"validator-a"}, enabled: true},
					&MockValidator{name: "validator-c", runAfter: []string{"validator-b"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render dependency arrows", func() {
				mermaid := resolver.ToMermaid()
				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("validator-b --> validator-a"))
				Expect(mermaid).To(ContainSubstring("validator-c --> validator-b"))
			})
		})

		Context("with complex dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "wif-check", runAfter: []string{}, enabled: true},
					&MockValidator{name: "api-enabled", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "quota-check", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "network-check", runAfter: []string{"api-enabled", "quota-check"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render all dependency relationships", func() {
				mermaid := resolver.ToMermaid()
				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("api-enabled --> wif-check"))
				Expect(mermaid).To(ContainSubstring("quota-check --> wif-check"))
				Expect(mermaid).To(ContainSubstring("network-check --> api-enabled"))
				Expect(mermaid).To(ContainSubstring("network-check --> quota-check"))
			})
		})

		Context("with missing dependency", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "validator-a", runAfter: []string{"non-existent"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should not render edges for missing dependencies", func() {
				mermaid := resolver.ToMermaid()
				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).NotTo(ContainSubstring("-->"))
				Expect(mermaid).NotTo(ContainSubstring("non-existent"))
			})
		})
	})

	Describe("ToMermaidWithLevels", func() {
		Context("with validators that have no dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "validator-a", runAfter: []string{}, enabled: true},
					&MockValidator{name: "validator-b", runAfter: []string{}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render all validators in Level 0 subgraph", func() {
				groups, _ := resolver.ResolveExecutionGroups()
				mermaid := resolver.ToMermaidWithLevels(groups)

				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 0 - 2 Validators in Parallel\""))
				Expect(mermaid).To(ContainSubstring("validator-a"))
				Expect(mermaid).To(ContainSubstring("validator-b"))
			})
		})

		Context("with linear dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "validator-a", runAfter: []string{}, enabled: true},
					&MockValidator{name: "validator-b", runAfter: []string{"validator-a"}, enabled: true},
					&MockValidator{name: "validator-c", runAfter: []string{"validator-b"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render separate levels with dependency arrows", func() {
				groups, _ := resolver.ResolveExecutionGroups()
				mermaid := resolver.ToMermaidWithLevels(groups)

				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 0\""))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 1\""))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 2\""))
				Expect(mermaid).To(ContainSubstring("validator-b --> validator-a"))
				Expect(mermaid).To(ContainSubstring("validator-c --> validator-b"))
			})
		})

		Context("with parallel dependencies", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "wif-check", runAfter: []string{}, enabled: true},
					&MockValidator{name: "api-enabled", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "quota-check", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "network-check", runAfter: []string{"wif-check"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should show parallel validators in the same level", func() {
				groups, _ := resolver.ResolveExecutionGroups()
				mermaid := resolver.ToMermaidWithLevels(groups)

				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 0\""))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 1 - 3 Validators in Parallel\""))
				Expect(mermaid).To(ContainSubstring("wif-check"))
				Expect(mermaid).To(ContainSubstring("api-enabled"))
				Expect(mermaid).To(ContainSubstring("quota-check"))
				Expect(mermaid).To(ContainSubstring("network-check"))
			})
		})

		Context("with complex dependency graph", func() {
			BeforeEach(func() {
				validators = []validator.Validator{
					&MockValidator{name: "wif-check", runAfter: []string{}, enabled: true},
					&MockValidator{name: "api-enabled", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "quota-check", runAfter: []string{"wif-check"}, enabled: true},
					&MockValidator{name: "iam-check", runAfter: []string{"api-enabled"}, enabled: true},
					&MockValidator{name: "network-check", runAfter: []string{"api-enabled", "quota-check"}, enabled: true},
				}
				resolver = validator.NewDependencyResolver(validators)
			})

			It("should render correct levels and all dependency edges", func() {
				groups, _ := resolver.ResolveExecutionGroups()
				mermaid := resolver.ToMermaidWithLevels(groups)

				Expect(mermaid).To(ContainSubstring("flowchart TD"))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 0\""))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 1 - 2 Validators in Parallel\""))
				Expect(mermaid).To(ContainSubstring("subgraph \"Level 2 - 2 Validators in Parallel\""))
				Expect(mermaid).To(ContainSubstring("api-enabled --> wif-check"))
				Expect(mermaid).To(ContainSubstring("quota-check --> wif-check"))
				Expect(mermaid).To(ContainSubstring("iam-check --> api-enabled"))
				Expect(mermaid).To(ContainSubstring("network-check --> api-enabled"))
				Expect(mermaid).To(ContainSubstring("network-check --> quota-check"))
			})
		})
	})
})
